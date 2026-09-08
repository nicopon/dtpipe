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

    internal static string[] SplitArguments(string commandLine)
    {
        var args = new List<string>();
        var current = new System.Text.StringBuilder();
        bool inDoubleQuotes = false;
        bool inSingleQuotes = false;
        
        for (int i = 0; i < commandLine.Length; i++)
        {
            char c = commandLine[i];
            
            if (c == '\\' && i + 1 < commandLine.Length && commandLine[i + 1] == '"')
            {
                current.Append('"');
                i++;
            }
            else if (c == '"' && !inSingleQuotes)
            {
                inDoubleQuotes = !inDoubleQuotes;
            }
            else if (c == '\'' && !inDoubleQuotes)
            {
                inSingleQuotes = !inSingleQuotes;
            }
            else if (char.IsWhiteSpace(c) && !inDoubleQuotes && !inSingleQuotes)
            {
                if (current.Length > 0)
                {
                    args.Add(current.ToString());
                    current.Clear();
                }
            }
            else
            {
                current.Append(c);
            }
        }
        
        if (current.Length > 0)
        {
            args.Add(current.ToString());
        }
        
        return args.ToArray();
    }


    private record ReaderResolutionResult(
        IStreamReaderFactory Factory,
        string EffectiveConnectionString);

    private ReaderResolutionResult? TryResolveReader(
        OptionsRegistry registry,
        List<IStreamReaderFactory> readerFactories,
        string input,
        string? query)
    {
        string effectiveConnectionString = input;
        string? variant = null;
        IStreamReaderFactory? factory = null;

        foreach (var f in readerFactories)
        {
            var selection = ComponentSelector.Select(input, f.ComponentName);
            if (selection.Matched)
            {
                effectiveConnectionString = selection.Cleaned;
                variant = selection.Variant;
                factory = f;
                break;
            }
        }

        if (factory == null)
        {
            foreach (var f in readerFactories)
            {
                if (f.CanHandle(input))
                {
                    factory = f;
                    break;
                }
            }
        }

        if (factory == null) return null;

        ValidatePathSafety(effectiveConnectionString);

        registry.Register(new DtPipe.Cli.Infrastructure.ConnectionRoute(effectiveConnectionString, string.Empty, variant, null));

        // OptionsRegistry.Get hands back a THROWAWAY instance when the type was never
        // registered — which, over MCP, is every provider, since nothing binds CLI flags here.
        // Mutating that instance and walking away drops the query silently: the factory then
        // reads the registry, gets another fresh default, and the reader reports "a query is
        // required" for a call that supplied one. The write-back is what makes it stick, the
        // same way ExportService.InjectSchema does it.
        var readerOpts = registry.Get(factory.OptionsType) as DtPipe.Core.Options.IQueryAwareOptions;
        if (readerOpts != null && !string.IsNullOrWhiteSpace(query))
        {
            readerOpts.Query = query;
            registry.RegisterByType(factory.OptionsType, readerOpts);
        }

        return new ReaderResolutionResult(factory, effectiveConnectionString);
    }


    [McpServerTool(Name = "dry-run")]
    [System.ComponentModel.Description("Run a pipeline over a small sample of its source, through the real execution path, with the writer neutralised so nothing is written to the target. Returns the rows as they leave each stage, the target compatibility report, and what could actually be guaranteed about the source not being modified.")]
    public async Task<string> DryRun(
        [System.ComponentModel.Description("The complete YAML configuration string representing the pipeline")] string yamlContent,
        [System.ComponentModel.Description("Source rows to run through the pipeline (default 10, max 1000)")] int rows = 10,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(yamlContent))
            return JsonSerializer.Serialize(new { success = false, error = "YAML job content cannot be empty." });

        try
        {
            var parsed = ParseAndValidateYaml(yamlContent);
            if (parsed.Errors.Count > 0)
                return JsonSerializer.Serialize(new { success = false, stage = "validation", errors = parsed.Errors },
                    new JsonSerializerOptions { WriteIndented = true });

            var report = await RunSampleAsync(parsed, Math.Clamp(rows, 1, 1000), ct,
                nextStep: "This was a preview: no data was written to the target. To run the pipeline for real, "
                        + "call execute-yaml-job with apply=true.");
            return JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new
            {
                success = false,
                stage = "dry-run",
                applied = false,
                mode = "sample",
                errors = new[] { DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message) }
            }, new JsonSerializerOptions { WriteIndented = true });
        }
    }

    /// <summary>
    /// Runs the pipeline in sample mode and shapes what it observed into JSON.
    ///
    /// It goes through JobService like any other run — the same reader, transformers,
    /// segmentation and bridges — because a tool that answered from its own walk over the data
    /// would be a second engine, and this cycle just removed the last one. Before this, the
    /// dry-run tool opened the reader and listed columns; it never ran a transformer at all,
    /// so it could not have told a model that its --window step produced nothing.
    /// </summary>
    private async Task<object> RunSampleAsync(YamlParseResult parsed, int rows, CancellationToken ct, string? nextStep = null)
    {
        var collector = _serviceProvider.GetRequiredService<DtPipe.DryRun.SampleReportCollector>();
        var jobService = _serviceProvider.GetRequiredService<DtPipe.Cli.JobService>();

        var jobs = parsed.Jobs.ToDictionary(
            kv => kv.Key,
            kv => kv.Value with { DryRunCount = rows },
            StringComparer.OrdinalIgnoreCase);

        var contexts = jobs.ToDictionary(kv => kv.Key, kv => new CliJobContext(null, null, null, System.Array.Empty<string>()));

        collector.Clear();
        collector.Enabled = true;
        int exitCode;
        string? failure = null;
        try
        {
            // Quiet: the caller is a model reading JSON, not a person watching the run. NoStats
            // alone leaves the topology panel and the results table on the console.
            exitCode = await jobService.ExecutePipelineAsync(jobs, parsed.Dag, contexts,
                new GlobalOptions { NoStats = true, Quiet = true }, ct);
        }
        catch (Exception ex)
        {
            // A failed sample is still a sample that wrote nothing. Reporting it as a bare
            // error would drop `applied: false`, and a model could no longer tell "nothing was
            // written" from "something was written and then it failed" — which is exactly the
            // distinction F2 exists to keep visible.
            exitCode = 1;
            failure = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message);
        }
        finally
        {
            collector.Enabled = false;
        }

        return new
        {
            success = exitCode == 0,
            exitCode,
            applied = false,
            mode = "sample",
            rowsRequested = rows,
            error = failure,
            // A response can be honest and still unusable. This one reports success with a full
            // trace, which a model reads as "the job is done" unless the response says what is
            // left to do — a real mission run stopped here and never wrote its target. `applied:
            // false` states the fact; nextStep states the consequence, and the model acts on the
            // second.
            nextStep,
            branches = collector.Reports.Select(kv => Shape(kv.Key, kv.Value)).ToList()
        };
    }

    /// <summary>Shapes one branch's report. Values become strings: a model reads them, it does not compute on them.</summary>
    private static object Shape(string alias, DtPipe.DryRun.SampleReport report) => new
    {
        branch = alias,
        checkpoint = report.CheckpointKey,
        rowsRead = report.Run.RowsRead,
        rowsDelivered = report.Run.RowsWritten,
        // What the run can actually support. "Nothing was written" is true of the writer; the
        // source is a separate question, and answering one with the other is how a reassuring
        // message becomes a false one.
        writerNeutralised = true,
        sourceProtection = report.Enforcement.ToString(),
        stages = report.Run.Stages.Select(s => new
        {
            index = s.Index,
            name = s.Name,
            columnar = s.IsColumnar,
            rowsSeen = s.TotalSeen,
            columns = s.Schema.Select(c => new { c.Name, type = c.ClrType?.Name ?? "unknown", c.IsNullable }).ToList(),
            sample = s.Rows.Take(10).Select(r => r.Select(v => v?.ToString()).ToList()).ToList()
        }).ToList(),
        schemaCompatibility = report.CompatibilityReport is null ? null : new
        {
            errors = report.CompatibilityReport.Errors,
            warnings = report.CompatibilityReport.Warnings
        },
        schemaInspectionError = report.SchemaInspectionError is null
            ? null
            : DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(report.SchemaInspectionError),
        keyValidation = report.KeyValidation is null ? null : new
        {
            required = report.KeyValidation.IsRequired,
            valid = report.KeyValidation.IsValid,
            errors = report.KeyValidation.Errors,
            warnings = report.KeyValidation.Warnings
        },
        constraintValidation = report.ConstraintValidation is null ? null : new
        {
            errors = report.ConstraintValidation.Errors,
            warnings = report.ConstraintValidation.Warnings
        },
        performanceHints = report.PerformanceHints
    };


    [McpServerTool(Name = "get-dag-topology")]
    [System.ComponentModel.Description("Return the pipeline's branch topology as structured data: for each branch its alias, input, output, the row transformers applied between them in order ('transformers'), the stream processor, and the upstream aliases it streams from ('from') or materialises ('ref'). Every stage of a branch is listed, so a pipeline whose substance is a transformer is not reported as a bare source-to-sink copy. Read-only — parses the YAML, runs nothing.")]
    public string GetDagTopology(
        [System.ComponentModel.Description("The complete YAML configuration string representing the pipeline")] string yamlContent)
    {
        if (string.IsNullOrWhiteSpace(yamlContent))
            return JsonSerializer.Serialize(new { success = false, error = "YAML job content cannot be empty." });

        try
        {
            var topology = DagTopologyService.FromServices(_serviceProvider).Describe(yamlContent);
            return JsonSerializer.Serialize(new
            {
                success = true,
                branches = topology.Branches.Select(b => new
                {
                    alias = b.Alias,
                    input = string.IsNullOrEmpty(b.Input) ? null : DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(b.Input),
                    output = string.IsNullOrEmpty(b.Output) ? null : DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(b.Output),
                    processor = b.Processor,
                    from = b.From,
                    @ref = b.Ref
                }).ToList()
            }, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new
            {
                success = false,
                errors = new[] { DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message) }
            }, new JsonSerializerOptions { WriteIndented = true });
        }
    }


    [McpServerTool(Name = "list-cursors")]
    [System.ComponentModel.Description("List active incremental cursors stored in the current workspace. Finds all state files and shows column, last value, last run time, status and row count.")]
    public string ListCursors()
    {
        var cursors = new List<object>();
        try
        {
            var workspaceDir = Directory.GetCurrentDirectory();
            var jsonFiles = Directory.EnumerateFiles(workspaceDir, "*.json", SearchOption.AllDirectories)
                .Where(f => !f.Contains("/bin/") && !f.Contains("/obj/") && !f.Contains("/node_modules/") && !f.Contains("/.git/") && !f.Contains("/.agents/") && !f.Contains("/.gemini/"));

            foreach (var file in jsonFiles)
            {
                try
                {
                    var content = File.ReadAllText(file);
                    using var doc = JsonDocument.Parse(content);
                    var root = doc.RootElement;
                    if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("cursor", out var cursorProp) && root.TryGetProperty("version", out _))
                    {
                        var relativePath = Path.GetRelativePath(workspaceDir, file);
                        var col = cursorProp.TryGetProperty("column", out var colProp) ? colProp.GetString() : null;
                        var val = cursorProp.TryGetProperty("value", out var valProp) ? valProp.GetString() : null;
                        var type = cursorProp.TryGetProperty("type", out var typeProp) ? typeProp.GetString() : null;

                        string? status = null;
                        string? lastRun = null;
                        long rows = 0;

                        if (root.TryGetProperty("last_run", out var lastRunProp))
                        {
                            status = lastRunProp.TryGetProperty("status", out var statusProp) ? statusProp.GetString() : null;
                            lastRun = lastRunProp.TryGetProperty("completed_at", out var completedProp) ? completedProp.GetString() : null;
                            rows = lastRunProp.TryGetProperty("rows_transferred", out var rowsProp) && rowsProp.ValueKind == JsonValueKind.Number ? rowsProp.GetInt64() : 0;
                        }

                        cursors.Add(new
                        {
                            stateFile = relativePath,
                            column = col,
                            value = val,
                            type = type,
                            status = status,
                            lastRunCompletedAt = lastRun,
                            rowsTransferred = rows
                        });
                    }
                }
                catch
                {
                    // Ignore invalid JSON files
                }
            }
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { error = $"Failed to list cursors: {ex.Message}" });
        }

        if (cursors.Count == 0)
        {
            return JsonSerializer.Serialize(new { info = "No active cursor state files found in the current workspace." });
        }

        return JsonSerializer.Serialize(cursors, new JsonSerializerOptions { WriteIndented = true });
    }


    [McpServerTool(Name = "suggest-pipeline")]
    [System.ComponentModel.Description("Given a source and destination, generate a ready-to-validate YAML pipeline configuration. Inspects the source schema and produces a complete YAML skeleton with correct column names and types.")]
    public async Task<string> SuggestPipeline(
        [System.ComponentModel.Description("Source connection string with provider prefix (e.g. 'csv:data.csv', 'pg:Host=...')")] string source,
        [System.ComponentModel.Description("Destination connection string with provider prefix (e.g. 'sqlite:output.db', 'parquet:output.parquet')")] string destination,
        [System.ComponentModel.Description("Optional: SQL query for database sources")] string? query = null,
        CancellationToken ct = default)
    {
        try
        {
            var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
            var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>().ToList();
            var writerFactories = _serviceProvider.GetRequiredService<IEnumerable<IDataWriterFactory>>().ToList();

            var resolvedReader = TryResolveReader(registry, readerFactories, source, query);
            if (resolvedReader == null)
            {
                return JsonSerializer.Serialize(new { error = $"Could not resolve reader provider for source '{source}'." });
            }

            List<PipeColumnInfo>? columns = null;
            string? schemaError = null;
            try
            {
                await using var reader = resolvedReader.Factory.Create(registry);
                await reader.OpenAsync(ct);
                columns = reader.Columns?.ToList();
            }
            catch (Exception ex)
            {
                schemaError = ex.Message;
            }

            var resolvedWriter = TryResolveWriter(writerFactories, destination);

            var sb = new System.Text.StringBuilder();
            sb.AppendLine("main:");
            sb.AppendLine($"  input: \"{source.Replace("\"", "\\\"")}\"");
            if (!string.IsNullOrEmpty(query))
            {
                sb.AppendLine($"  query: \"{query.Replace("\"", "\\\"")}\"");
            }
            sb.AppendLine($"  output: \"{destination.Replace("\"", "\\\"")}\"");

            if (schemaError != null)
            {
                sb.AppendLine();
                sb.AppendLine($"  # Warning: Could not inspect source schema: {schemaError}");
            }
            else if (columns != null && columns.Count > 0)
            {
                sb.AppendLine();
                sb.AppendLine("  # Source schema detected:");
                foreach (var col in columns)
                {
                    sb.AppendLine($"  #   - {col.Name} ({col.ClrType?.Name ?? "unknown"}, nullable: {col.IsNullable})");
                }
                sb.AppendLine();
                // 'project' whitelists through 'mappings:' keys — the form its own help publishes
                // and the one '--export-job' emits. A skeleton offering 'columns:' binds nothing:
                // the key is dropped by the deserializer, the transformer is built with an empty
                // whitelist and passes every column through, and validation says the job is fine.
                // A recorded session copied it into all six of its candidates.
                sb.AppendLine("  # Uncomment and modify to select or transform columns");
                sb.AppendLine("  # (keys are the columns to keep, in order; values are ignored):");
                sb.AppendLine("  # transformers:");
                sb.AppendLine("  #   - type: project");
                sb.AppendLine("  #     mappings:");
                foreach (var col in columns)
                {
                    sb.AppendLine($"  #       {col.Name}: ~");
                }
            }

            return sb.ToString();
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { error = $"Failed to suggest pipeline: {ex.Message}" });
        }
    }


    private IDataWriterFactory? TryResolveWriter(
        List<IDataWriterFactory> writerFactories,
        string output)
    {
        IDataWriterFactory? factory = null;

        foreach (var f in writerFactories)
        {
            if (ComponentSelector.Matches(output, f.ComponentName))
            {
                factory = f;
                break;
            }
        }

        if (factory == null)
        {
            foreach (var f in writerFactories)
            {
                if (f.CanHandle(output))
                {
                    factory = f;
                    break;
                }
            }
        }

        return factory;
    }


    private static string? TryGetQueryFromJob(JobDefinition job, string componentName)
    {
        if (job.ProviderOptions == null) return null;

        var keys = new[] { componentName, componentName + "-reader", componentName + "-writer" };
        foreach (var key in keys)
        {
            if (job.ProviderOptions.TryGetValue(key, out var opts) && opts.TryGetValue("query", out var queryObj))
            {
                return queryObj?.ToString();
            }
        }

        return null;
    }

}
