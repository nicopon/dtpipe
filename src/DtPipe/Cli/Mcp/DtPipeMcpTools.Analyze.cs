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
        if (!string.IsNullOrWhiteSpace(query))
        {
            // No else here meant the capability interface acted as a silent filter: a query aimed
            // at a reader that takes none was dropped, and the tool answered about the whole
            // source as though it had been narrowed. Say so instead — a wrong answer that looks
            // right is the one kind of failure a model cannot recover from.
            if (readerOpts == null)
                throw new InvalidOperationException(
                    $"The '{factory.ComponentName}' reader takes no query, so the one supplied would be ignored and "
                  + $"the whole source read instead. Narrow it downstream — a 'filter' transformer, or a '--sql' "
                  + $"branch reading this one — and call 'get-adapter-help' for what '{factory.ComponentName}' does accept.");

            readerOpts.Query = query;
            registry.RegisterByType(factory.OptionsType, readerOpts);
        }

        return new ReaderResolutionResult(factory, effectiveConnectionString);
    }


    [McpServerTool(Name = "dry-run")]
    [System.ComponentModel.Description("Run a pipeline over a small sample of its source, through the real execution path, with the writer neutralised so nothing is written to the target. Returns the rows as they leave each stage, the target compatibility report, and what could actually be guaranteed about the source not being modified.")]
    public Task<string> DryRun(
        [System.ComponentModel.Description("The complete YAML configuration string representing the pipeline")] string yamlContent,
        [System.ComponentModel.Description("Source rows to run through the pipeline (default 10, max 1000)")] int rows = 10,
        CancellationToken ct = default) => _analysis.SampleAsync(yamlContent, rows, shownRows: 10, ct,
            // Not a pointer to 'execute-yaml-job': in plan mode that tool is hidden and the role
            // prompt forbids it, so naming it here sends the reader after something it cannot
            // reach — the same mistake 'help' made and had removed.
            nextStep: "This was a preview: nothing was written to the target. Running the pipeline "
                    + "for real is a separate, deliberate step.",
            hints: McpHints);

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
                    input = string.IsNullOrEmpty(b.Input) ? null : DtPipe.Core.Security.ConnectionStringSanitizer.Redact(b.Input),
                    output = string.IsNullOrEmpty(b.Output) ? null : DtPipe.Core.Security.ConnectionStringSanitizer.Redact(b.Output),
                    processor = b.Processor,
                    // The description promises this key by name and says why it is there. It was
                    // absent from the projection, so every branch read as a bare source-to-sink
                    // copy — the exact failure the description says the tool avoids.
                    transformers = b.Transformers,
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
