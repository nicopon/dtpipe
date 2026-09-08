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
using ModelContextProtocol.Server;
namespace DtPipe.Cli.Mcp;

public partial class DtPipeMcpTools
{

    private record YamlParseResult(
        Dictionary<string, JobDefinition> Jobs,
        JobDagDefinition Dag,
        List<string> Errors);

    private YamlParseResult ParseAndValidateYaml(string yamlContent)
    {
        var build = DagTopologyService.FromServices(_serviceProvider).Build(yamlContent);

        var streamTransformerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
        var errors = PipelineValidator.Validate(build.Dag, build.Jobs, streamTransformerFactories).ToList();
        errors.AddRange(BranchesWithNothingToRead(build.Dag, build.Jobs, streamTransformerFactories));
        errors.AddRange(ValidateJobTransformers(build.Jobs));
        errors.AddRange(ValidateProviderOptionKeys(build.Jobs));

        return new YamlParseResult(build.Jobs, build.Dag, errors);
    }

    /// <summary>
    /// The rule the engine applies when it resolves a reader: a branch reads from its
    /// <c>input:</c>, or from a stream processor claiming it. Neither means the run stops before a
    /// row moves.
    ///
    /// <para>
    /// It lives on the YAML-content path only. A branch built from command-line arguments carries
    /// its input elsewhere — a channel endpoint, a resumed checkpoint — so asking the same question
    /// of <see cref="PipelineValidator"/>, which both paths share, refused pipelines that run.
    /// </para>
    ///
    /// <para>
    /// A recorded session invented a job shaped <c>job: / sources: / transformers:</c>, was told it
    /// was valid, and spent the rest of its turn re-reading a plan that could never run: a model
    /// with no error to act on has nothing to correct.
    /// </para>
    /// </summary>
    private static IEnumerable<string> BranchesWithNothingToRead(
        JobDagDefinition dag, Dictionary<string, JobDefinition> jobs, IEnumerable<IStreamTransformerFactory> processorFactories)
    {
        var factories = processorFactories.ToList();

        foreach (var branch in dag.Branches)
        {
            if (!jobs.TryGetValue(branch.Alias, out var job)) continue;
            if (!string.IsNullOrWhiteSpace(job.Input)) continue;
            if (factories.Any(f => f.IsApplicable(job))) continue;

            yield return $"Branch '{branch.Alias}' has nothing to read: it has no 'input:' and no stream "
                       + "processor. Give it an 'input:', or — when it combines other branches through "
                       + "'from'/'ref' — a query under 'provider-options -> sql -> query'.";
        }
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
            var parsed = ParseAndValidateYaml(yamlContent);

            if (parsed.Errors.Count > 0)
            {
                return JsonSerializer.Serialize(new
                {
                    success = false,
                    errors = parsed.Errors
                }, new JsonSerializerOptions { WriteIndented = true });
            }

            return JsonSerializer.Serialize(new
            {
                success = true,
                message = "YAML job configuration and topology are valid."
            }, new JsonSerializerOptions { WriteIndented = true });
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
            var parsed = ParseAndValidateYaml(yamlContent);

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

              var safety = DefaultSqlSafetyPolicy.DryRunYaml(yamlContent, new SqlSafetyOptions
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
                       var sample = await RunSampleAsync(parsed, rows: 10, ct,
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
                    Input = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(b.Input),
                    Output = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(b.Output)
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
    /// Checks every key under <c>provider-options</c> against the options it would bind to.
    ///
    /// <para>
    /// This is the one class of mistake an agent cannot otherwise learn from. A key that binds to
    /// nothing is a warning on the engine's stderr, which never reaches a tool result — so the run
    /// succeeds, quietly using the default, and the trace of the session shows nothing wrong. The
    /// validator is where a caller comes to be told what is wrong before running, so it is where
    /// this belongs.
    /// </para>
    ///
    /// <para>
    /// A plain block (<c>csv:</c>) may legitimately carry keys for only one side of a reader/writer
    /// pair, so it is checked against both; a suffixed block (<c>csv-writer:</c>) is checked against
    /// that side alone.
    /// </para>
    /// </summary>
    private List<string> ValidateProviderOptionKeys(Dictionary<string, JobDefinition> jobs)
    {
        var errors = new List<string>();
        // The factories this instance was built with, not the container's: they are the same set,
        // and asking the container for a collection nobody registered returns an empty one rather
        // than failing, which would silently turn every check below into a no-op.
        var factories = _readerFactories.Cast<DtPipe.Core.Abstractions.IDataFactory>()
            .Concat(_writerFactories).ToList();

        foreach (var (alias, job) in jobs)
        {
            if (job.ProviderOptions is null) continue;

            foreach (var (block, options) in job.ProviderOptions)
            {
                if (options is null || options.Count == 0) continue;

                var (component, writerOnly, readerOnly) = SplitBlock(block);
                var targets = factories
                    .Where(f => f.ComponentName.Equals(component, StringComparison.OrdinalIgnoreCase))
                    .Where(f => !(writerOnly && f is not DtPipe.Core.Abstractions.IDataWriterFactory)
                             && !(readerOnly && f is DtPipe.Core.Abstractions.IDataWriterFactory))
                    .ToList();

                // Silence when the block matches no registered factory. A container assembled for
                // one purpose does not register every provider, and absence of a factory is not
                // evidence of a wrong key — only a factory we found tells us what the valid keys are.
                if (targets.Count == 0) continue;

                var known = targets
                    .SelectMany(t => t.OptionsType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
                    .Where(p => p.CanWrite)
                    .Select(p => p.Name.Replace("-", string.Empty).Replace("_", string.Empty).ToLowerInvariant())
                    .ToHashSet(StringComparer.Ordinal);

                foreach (var key in options.Keys)
                {
                    if (known.Contains(key.Replace("-", string.Empty).Replace("_", string.Empty).ToLowerInvariant()))
                        continue;

                    errors.Add($"Branch '{alias}', provider-options '{block}': "
                             + Pipeline.OptionBinder.DescribeUnknownKey(targets[0].OptionsType, key));
                }
            }
        }

        return errors;
    }

    /// <summary>A provider-options block name split into its component and the side it pins, if any.</summary>
    private static (string Component, bool WriterOnly, bool ReaderOnly) SplitBlock(string block) =>
        block.EndsWith("-writer", StringComparison.OrdinalIgnoreCase) ? (block[..^7], true, false)
        : block.EndsWith("-reader", StringComparison.OrdinalIgnoreCase) ? (block[..^7], false, true)
        : (block, false, false);

    /// <summary>
    /// A transformer is checked by building it, which is what the engine does with the same
    /// factory and the same <see cref="YamlTransformerBuilder"/> — construction only, no reader,
    /// no connection, no row.
    ///
    /// <para>
    /// Checking the type name alone made the validator weaker than the engine on everything a
    /// transformer rejects at construction: a faker method that does not exist, a rename that is
    /// not 'Old:New', an options block that produces nothing. A recorded session was told a job
    /// was valid and delivered it; the run died on 'Unknown faker method' before a row moved.
    /// </para>
    /// </summary>
    private List<string> ValidateJobTransformers(Dictionary<string, JobDefinition> jobs)
    {
        var errors = new List<string>();
        var transformerFactories = _serviceProvider.GetRequiredService<IEnumerable<IDataTransformerFactory>>().ToList();

        foreach (var (alias, job) in jobs)
        {
            foreach (var t in job.Transformers ?? new List<Core.Pipelines.TransformerConfig>())
            {
                if (string.IsNullOrEmpty(t.Type)) continue;

                var factory = transformerFactories.FirstOrDefault(f => f.ComponentName.Equals(t.Type, StringComparison.OrdinalIgnoreCase));
                if (factory is null)
                {
                    errors.Add($"Branch '{alias}': Unknown transformer type '{t.Type}'. Call 'list-providers' to see valid transformer types (e.g., 'fake', 'compute', 'filter', 'project'). Note: Joins across data sources are configured using the 'sql' provider under 'provider-options', not as a transformer type.");
                    continue;
                }

                try
                {
                    (YamlTransformerBuilder.Build(factory, t) as IDisposable)?.Dispose();
                }
                catch (Exception ex)
                {
                    errors.Add($"Branch '{alias}', transformer '{t.Type}': {ToolError.Describe(ex)}");
                }
            }
        }

        return errors;
    }

}
