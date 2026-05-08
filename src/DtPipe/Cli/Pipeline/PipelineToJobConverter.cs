using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Pipeline;

public static class PipelineToJobConverter
{
    public static (Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag, Dictionary<string, CliJobContext> Contexts) Convert(
        ParsedPipeline parsed,
        IEnumerable<IStreamTransformerFactory>? streamTransformerFactories = null)
    {
        // --job mode: load from YAML file and apply CLI overrides
        if (!string.IsNullOrEmpty(parsed.Globals.JobFile))
            return ConvertFromJobFile(parsed, streamTransformerFactories);

        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase);
        var contexts = new Dictionary<string, CliJobContext>(StringComparer.OrdinalIgnoreCase);
        var branches = new List<BranchDefinition>();

        // Pass 1: Collect explicit aliases to avoid collisions
        var explicitAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var b in parsed.Branches)
        {
            if (!string.IsNullOrEmpty(b.Alias))
                explicitAliases.Add(b.Alias);
        }

        var processorFactories = streamTransformerFactories?.ToList();

        int branchCounter = 1;
        foreach (var branchSpec in parsed.Branches)
        {
            var alias = branchSpec.Alias;
            if (string.IsNullOrEmpty(alias))
            {
                if (parsed.Branches.Count == 1)
                {
                    alias = "main";
                }
                else
                {
                    while (explicitAliases.Contains($"stream{branchCounter}"))
                        branchCounter++;
                    alias = $"stream{branchCounter}";
                    branchCounter++;
                }
            }

            var job = MapToJobDefinition(parsed.Globals, branchSpec);

            var processorName = processorFactories?
                .FirstOrDefault(f => f.IsApplicable(branchSpec.RawArgs))
                ?.ComponentName;

            jobs[alias] = job;
            contexts[alias] = new CliJobContext(branchSpec.ReaderArgs, branchSpec.PipelineArgs, branchSpec.WriterArgs, branchSpec.RawArgs);
            branches.Add(new BranchDefinition
            {
                Alias = alias,
                Input = job.Input,
                Output = job.Output,
                StreamingAliases = branchSpec.From.ToArray(),
                RefAliases = branchSpec.Ref.ToArray(),
                Arguments = branchSpec.RawArgs,
                ProcessorName = processorName
            });
        }

        var dag = new JobDagDefinition { Branches = branches };
        return (jobs, dag, contexts);
    }

    private static (Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag, Dictionary<string, CliJobContext> Contexts) ConvertFromJobFile(
        ParsedPipeline parsed,
        IEnumerable<IStreamTransformerFactory>? streamTransformerFactories)
    {
        var jobs = JobFileParser.Parse(parsed.Globals.JobFile!);
        var flags = parsed.Globals.AllFlags;

        // Apply CLI overrides to all loaded jobs
        int? limitOverride = GetInt(flags, "--limit");
        int? batchOverride = GetInt(flags, "--batch-size", "-b");
        string? logOverride = GetString(flags, "--log");
        string? metricsOverride = GetString(flags, "--metrics-path");

        foreach (var alias in jobs.Keys.ToList())
        {
            var job = jobs[alias];
            if (parsed.Globals.DryRunCount > 0) job = job with { DryRunCount = parsed.Globals.DryRunCount };
            if (limitOverride is > 0)           job = job with { Limit = limitOverride.Value };
            if (batchOverride is > 0)           job = job with { BatchSize = batchOverride.Value };
            if (!string.IsNullOrEmpty(logOverride))     job = job with { LogPath = logOverride };
            if (!string.IsNullOrEmpty(metricsOverride)) job = job with { MetricsPath = metricsOverride };
            jobs[alias] = job;
        }

        var branches = jobs.Select(kv => new BranchDefinition
        {
            Alias = kv.Key,
            Input = kv.Value.Input,
            Output = kv.Value.Output,
            StreamingAliases = kv.Value.From != null ? new[] { kv.Value.From } : Array.Empty<string>(),
            RefAliases = kv.Value.Ref ?? Array.Empty<string>(),
            Arguments = Array.Empty<string>(),
            ProcessorName = streamTransformerFactories?
                .FirstOrDefault(f => kv.Value.ProviderOptions?.ContainsKey(f.ComponentName) == true)
                ?.ComponentName
        }).ToList();

        return (jobs, new JobDagDefinition { Branches = branches }, new Dictionary<string, CliJobContext>(StringComparer.OrdinalIgnoreCase));
    }

    private static JobDefinition MapToJobDefinition(GlobalOptions globals, BranchSpec branch)
    {
        // Engine-control values are extracted from AllFlags (global) and Flags (branch-local).
        // Branch-local values take precedence over global defaults.
        int batchSize = GetInt(branch.Flags, "--batch-size", "-b")
                     ?? GetInt(globals.AllFlags, "--batch-size", "-b")
                     ?? 50_000;
        int limit = GetInt(branch.Flags, "--limit")
                 ?? GetInt(globals.AllFlags, "--limit")
                 ?? 0;
        double samplingRate = GetDouble(branch.Flags, "--sampling-rate", "--sample-rate")
                           ?? GetDouble(globals.AllFlags, "--sampling-rate", "--sample-rate")
                           ?? 1.0;
        int? samplingSeed = GetNullableInt(branch.Flags, "--sampling-seed", "--sample-seed")
                        ?? GetNullableInt(globals.AllFlags, "--sampling-seed", "--sample-seed");
        string? logPath = GetString(branch.Flags, "--log") ?? globals.LogPath;
        string? metricsPath = GetString(branch.Flags, "--metrics-path")
                           ?? GetString(globals.AllFlags, "--metrics-path");
        string? prefix = GetString(branch.Flags, "--prefix", "-p")
                      ?? GetString(globals.AllFlags, "--prefix", "-p");

        return new JobDefinition
        {
            Input  = branch.Input,
            Output = branch.Output,
            BatchSize    = batchSize,
            DryRunCount  = globals.DryRunCount,
            Limit        = limit,
            SamplingRate = samplingRate,
            SamplingSeed = samplingSeed,
            LogPath      = logPath,
            MetricsPath  = metricsPath,
            Prefix       = prefix,
            NoStats      = globals.NoStats,

            From     = branch.From.FirstOrDefault(),
            Ref      = branch.Ref.ToArray(),

            Transformers    = new List<TransformerConfig>(),
            ProviderOptions = new Dictionary<string, Dictionary<string, object>>(StringComparer.OrdinalIgnoreCase)
        };
    }

    // ── Helpers to extract typed values from flag dictionaries ──────────────

    private static string? GetString(IReadOnlyDictionary<string, List<string>> flags, params string[] keys)
    {
        foreach (var k in keys)
            if (flags.TryGetValue(k, out var list) && list.Count > 0) return list.Last();
        return null;
    }

    private static string? GetString(IReadOnlyDictionary<string, object?> flags, params string[] keys)
    {
        foreach (var k in keys)
            if (flags.TryGetValue(k, out var val)) return val?.ToString();
        return null;
    }

    private static int? GetInt(IReadOnlyDictionary<string, List<string>> flags, params string[] keys)
    {
        var s = GetString(flags, keys);
        return s != null && int.TryParse(s, out var v) ? v : null;
    }

    private static int? GetInt(IReadOnlyDictionary<string, object?> flags, params string[] keys)
    {
        var s = GetString(flags, keys);
        return s != null && int.TryParse(s, out var v) ? v : null;
    }

    private static int? GetNullableInt(IReadOnlyDictionary<string, List<string>> flags, params string[] keys)
        => GetInt(flags, keys);

    private static int? GetNullableInt(IReadOnlyDictionary<string, object?> flags, params string[] keys)
        => GetInt(flags, keys);

    private static double? GetDouble(IReadOnlyDictionary<string, List<string>> flags, params string[] keys)
    {
        var s = GetString(flags, keys);
        return s != null && double.TryParse(s, System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : null;
    }

    private static double? GetDouble(IReadOnlyDictionary<string, object?> flags, params string[] keys)
    {
        var s = GetString(flags, keys);
        return s != null && double.TryParse(s, System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : null;
    }
}
