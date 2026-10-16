using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Configuration;
using DtPipe.Cli;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Security;
using DtPipe.Core.Security;
using ModelContextProtocol.Server;
namespace DtPipe.Cli.Mcp;

public partial class DtPipeMcpTools
{

    /// <summary>The pointers a refusal gives to the tools of this server that answer it.</summary>
    private static readonly JobHints McpHints = new(
        " Call 'list-providers' to see valid transformer types (e.g., 'fake', 'compute', 'filter', 'project').",
        HelpPointerFor);

    /// <summary>
    /// What the job is shaped like, returned beside the verdict.
    ///
    /// <para>
    /// Strictly descriptive: how many branches, which read another, which targets they share. A
    /// recorded session built three branches whose foreign keys were <c>Math.random()</c> because
    /// nothing ever showed it that its branches were independent — the <c>from</c>/<c>ref</c>
    /// grammar was in 'help', nine iterations earlier and in the abstract. Saying what was built,
    /// in the tool the model actually returns to, is not advice; it is the shape it just made.
    /// </para>
    /// </summary>
    private static object Shape(ParsedJob parsed)
    {
        var branches = parsed.Dag.Branches
            .Select(b => new
            {
                alias = b.Alias,
                reads = parsed.Jobs.TryGetValue(b.Alias, out var j) && !string.IsNullOrEmpty(j.Input)
                    ? ConnectionStringSanitizer.Redact(j.Input) : null,
                writes = parsed.Jobs.TryGetValue(b.Alias, out var w) && !string.IsNullOrEmpty(w.Output)
                    ? ConnectionStringSanitizer.Redact(w.Output) : null,
                from = parsed.Jobs.TryGetValue(b.Alias, out var f) && !string.IsNullOrEmpty(f.From) ? f.From : null,
                @ref = parsed.Jobs.TryGetValue(b.Alias, out var r) ? r.Ref : Array.Empty<string>()
            })
            .ToList();

        var linked = branches.Count(b => b.from is not null || b.@ref.Length > 0);
        var sharedTargets = branches
            .Where(b => b.writes is not null)
            .GroupBy(b => b.writes!, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => new { target = g.Key, branches = g.Select(b => b.alias).ToList() })
            .ToList();

        return new
        {
            branches,
            branchesReadingAnother = linked,
            independent = branches.Count - linked,
            sharedTargets
        };
    }

    [McpServerTool(Name = "validate-yaml-job")]
    [System.ComponentModel.Description("Validate a pipeline configuration specified directly as YAML. Checks for syntax errors and schema validation issues without executing.")]
    public string ValidateYamlJob(
        [System.ComponentModel.Description("The complete YAML configuration string representing the pipeline")] string yamlContent)
    {
        if (string.IsNullOrWhiteSpace(yamlContent))
            return JsonSerializer.Serialize(new { success = false, error = "YAML job content cannot be empty." });

        try
        {
            var parsed = _analysis.Parse(yamlContent, hints: McpHints);

            if (parsed.Errors.Count > 0)
            {
                return JsonSerializer.Serialize(new
                {
                    success = false,
                    errors = parsed.Errors
                }, new JsonSerializerOptions { WriteIndented = true });
            }

            var answer = new Dictionary<string, object?>
            {
                ["success"] = true,
                ["message"] = "YAML job configuration and topology are valid.",
                ["checked"] = "Syntax, branch topology, provider-option keys, and every transformer built the way "
                            + "the engine builds it.",
                ["notChecked"] = "The source is never opened here, so no column name is verified: a transformer "
                               + "naming a column the source does not carry fails when the job runs, not now. "
                               + "Call 'dry-run' on this same YAML to check it against the real schema.",
                ["shape"] = Shape(parsed),
            };

            // A valid job that will silently do the wrong thing. Carried beside the verdict rather
            // than folded into it: the job IS valid, and demoting it to an error would refuse a
            // pipeline whose author wants tracking without filtering. Absent when there is nothing
            // to say, so the key means something wherever it appears.
            var advisories = DtPipe.Cli.Incremental.CursorAdvisory.Advise(parsed.Jobs, yamlContent);
            if (advisories.Count > 0) answer["advisories"] = advisories;

            return JsonSerializer.Serialize(answer, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new
            {
                success = false,
                errors = new[] { ToolError.Describe(ex, yamlContent) }
            }, new JsonSerializerOptions { WriteIndented = true });
        }
    }


     [McpServerTool(Name = "execute-yaml-job")]
     [WritesToTarget]
     [System.ComponentModel.Description("Execute a pipeline configuration specified directly as YAML. This is the only way to run pipelines and avoids command-line quoting/escaping issues. By default the run is a dry-run (no data is written); pass apply=true to perform a real write, which additionally requires approval and a compliant SQL safety check.")]
     public async Task<string> ExecuteYamlJob(
          [System.ComponentModel.Description("The complete YAML configuration string representing the pipeline")] string yamlContent,
          [System.ComponentModel.Description("Perform a real write. Default false => dry-run only.")] bool apply = false,
          [System.ComponentModel.Description("Allow destructive SQL verbs (DROP/DELETE/TRUNCATE/UPDATE/ALTER/INSERT/ATTACH). Default deny.")] bool allowDestructive = false,
          [System.ComponentModel.Description("Allow network access in SQL (LOAD httpfs/azure, remote read_parquet). Default deny.")] bool allowNetwork = false,
         CancellationToken ct = default)
      {
         if (string.IsNullOrWhiteSpace(yamlContent))
             return JsonSerializer.Serialize(new { success = false, error = "YAML job content cannot be empty." });

         try
          {
            var parsed = _analysis.Parse(yamlContent, hints: McpHints);

            if (parsed.Errors.Count > 0)
                {
                 return JsonSerializer.Serialize(new
                   {
                    success = false,
                    stage = "validation",
                    errors = parsed.Errors
                   }, new JsonSerializerOptions { WriteIndented = true });
                }

                  // F2 — guardrails. Fail-closed: by default nothing is written. A real write requires
                  // the apply flag, an approving gate, and a clean SQL safety check.
                  // When the agent set options, they are the source of truth for the safety policy and
                  // the approval override; the tool parameters are the MCP/standalone defaults.
              var agentOpts = AgentOptions;
             bool effectiveAllowDestructive = agentOpts?.AllowDestructive ?? allowDestructive;
             bool effectiveAllowNetwork = agentOpts?.AllowNetwork ?? allowNetwork;

              var safety = _analysis.CheckSafety(parsed, new SqlSafetyOptions
                    {
                  AllowDestructive = effectiveAllowDestructive,
                  AllowNetwork = effectiveAllowNetwork
                   });

             if (!safety.Allowed)
                   {
                   return JsonSerializer.Serialize(new
                      {
                      success = false,
                      stage = "safety",
                      applied = false,
                      violation = safety.Violations,
                      message = "Execution blocked by the SQL safety policy. Re-run with the appropriate --allow-* flag."
                       }, new JsonSerializerOptions { WriteIndented = true });
                   }

                   // Determine whether the operator consented to a real write. In the agent context
                   // the --apply flag (carried via AgentOptions) is the authoritative consent; outside
                   // the agent it comes from the tool parameter.
                   bool consentedApply = AgentOptions != null ? AgentOptions.Apply : apply;

                   // A real write requires consent. In the agent context the --apply flag IS the
                   // non-interactive approval. Outside the agent, a shared approval gate (fail-closed,
                   // denies non-interactive writes) makes the decision.
                   if (consentedApply && AgentOptions == null)
                      {
                       var approvalGate = _serviceProvider.GetService<IApprovalGate>() ?? new DefaultApprovalGate();
                       var request = new ApprovalRequest
                           {
                           Yaml = yamlContent,
                           Apply = true,
                           Interactive = false,
                           Description = "execute-yaml-job"
                           };

                        if (!approvalGate.Approve(request))
                            {
                          return JsonSerializer.Serialize(new
                             {
                              success = true,
                              stage = "approval",
                              applied = false,
                              mode = "dry-run",
                              message = "Write not approved (non-interactive). No data written — run was a dry-run only."
                                }, new JsonSerializerOptions { WriteIndented = true });
                            }
                       }

                   // apply=false => a real sample run, not an absence of one. The safety no longer
                    // comes from abstaining: the sample-mode sink cannot reach a user target
                    // (SampleModeSinkTests), and SampleModeSafetyGate refuses a source that could
                    // mutate. Fail-closed is preserved and the model finally learns what the
                    // pipeline would actually do — which is the loop this cycle exists to close.
                if (!consentedApply)
                        {
                       var sample = await _analysis.RunSampleAsync(parsed, rows: 10, shownRows: 10, ct,
                           nextStep: "NOTHING WAS WRITTEN. This ran the pipeline over a sample with the writer "
                                   + "neutralised, so you can see what it would do. The target file or table has "
                                   + "NOT been created or modified. Call execute-yaml-job again with apply=true to "
                                   + "perform the real write.");
                       return JsonSerializer.Serialize(sample, new JsonSerializerOptions { WriteIndented = true });
                           }

            var contexts = parsed.Jobs.ToDictionary(
                 kv => kv.Key,
                 kv => new CliJobContext(null, null, null, Array.Empty<string>())
               );

            var jobService = _serviceProvider.GetRequiredService<DtPipe.Cli.JobService>();
            var sw = System.Diagnostics.Stopwatch.StartNew();

            // Quiet: this result goes back as JSON. The topology panel and the live progress
            // display are for a person watching a CLI run, and under the agent's full-screen
            // surface they would land on a screen the toolkit owns.
            var globalOptions = new GlobalOptions { Quiet = true };
            var exitCode = await jobService.ExecutePipelineAsync(parsed.Jobs, parsed.Dag, contexts, globalOptions, ct);
            sw.Stop();

            return JsonSerializer.Serialize(new
               {
                success = exitCode == 0,
                exitCode,
                applied = true,
                mode = "write",
                durationMs = sw.ElapsedMilliseconds,
                safety = "ok",
                branches = parsed.Dag.Branches.Select(b => new
                   {
                    b.Alias,
                    Input = DtPipe.Core.Security.ConnectionStringSanitizer.Redact(b.Input),
                    Output = DtPipe.Core.Security.ConnectionStringSanitizer.Redact(b.Output)
                   }).ToList()
               }, new JsonSerializerOptions { WriteIndented = true });
           }
         catch (Exception ex)
           {
             bool consented = AgentOptions?.Apply ?? apply;
             return JsonSerializer.Serialize(new
               {
                 success = false,
                 stage = "execution",
                 // Fail-closed stays legible even on the error path: whatever went wrong, this
                 // call did not consent to a write.
                 applied = consented,
                 // And so does the remedy. A model that is only told the run failed cannot tell
                 // whether writing is still pending — every apply=false answer says the same two
                 // things, on the paths that worked and the ones that did not.
                 nextStep = consented
                     ? null
                     : "NOTHING WAS WRITTEN. This call ran with apply=false, so the target was never going "
                       + "to be modified. Fix the errors above, then call execute-yaml-job again with "
                       + "apply=true to perform the real write.",
                 errors = new[] { ToolError.Describe(ex, yamlContent) }
               }, new JsonSerializerOptions { WriteIndented = true });
           }
       }


    internal static void ValidatePathSafety(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) return;

        // Clean query/parameters from SQLite/DuckDB connection strings. This also covers
        // "duck+{provider}:" hub connection strings (e.g. "duck+mysql:Host=...;Database=...;") —
        // they are relational connection strings, not file paths, and are already recognized by
        // the Host=/Server=/User Id=/Database= check below. An earlier blanket
        // StartsWith("duck+") bypass skipped this check unconditionally regardless of content,
        // exempting the whole class by prefix instead of by shape.
        string cleanPath = path;
        int semicolonIndex = cleanPath.IndexOf(';');
        if (semicolonIndex >= 0)
        {
            // If it's a connection string containing key=value pairs, skip file path check
            if (cleanPath.Contains("Host=", StringComparison.OrdinalIgnoreCase) ||
                cleanPath.Contains("Server=", StringComparison.OrdinalIgnoreCase) ||
                cleanPath.Contains("User Id=", StringComparison.OrdinalIgnoreCase) ||
                cleanPath.Contains("Database=", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }
            // Otherwise, it might be an SQLite/DuckDB file connection string like "Data Source=filename;..."
            // Extract the path if it contains "Data Source="
            var match = Regex.Match(cleanPath, @"Data\s+Source\s*=\s*(?<file>[^;]+)", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                cleanPath = match.Groups["file"].Value.Trim();
            }
            else
            {
                cleanPath = cleanPath.Substring(0, semicolonIndex).Trim();
            }
        }

        // Strip quotes if any
        cleanPath = cleanPath.Trim('"', '\'');

        // Skip check if it is clearly in-memory
        if (string.Equals(cleanPath, ":memory:", StringComparison.OrdinalIgnoreCase) || string.Equals(cleanPath, "-", StringComparison.Ordinal))
        {
            return;
        }

        try
        {
            string fullPath = Path.GetFullPath(cleanPath);
            string currentDir = Path.GetFullPath(Directory.GetCurrentDirectory());

            if (!fullPath.StartsWith(currentDir, StringComparison.OrdinalIgnoreCase))
            {
                throw new UnauthorizedAccessException($"Access to path '{path}' is denied. Only files within the current workspace are accessible.");
            }
        }
        catch (UnauthorizedAccessException)
        {
            throw;
        }
        catch (Exception)
        {
            // If Path.GetFullPath throws, it might be a database connection string with invalid characters.
            // We ignore it here and let the provider handle/reject it.
        }
    }


    /// <summary>
    /// The tool that answers the question a refusal raises, named where naming it is true.
    ///
    /// <para>
    /// The transformer cannot say this itself: it is reached from the command line too, where no
    /// such tool exists — that is how the message came to cite '--fake-list', a flag this binary
    /// never had. Here the catalogue is the caller's own, so the pointer holds.
    /// </para>
    /// </summary>
    private static string HelpPointerFor(string transformerType) =>
        transformerType.Equals("fake", StringComparison.OrdinalIgnoreCase)
            ? " Call 'get-anonymization-help' for every dataset and method at once."
            : $" Call 'get-transformer-help' with '{transformerType}' for its options and examples.";

}
