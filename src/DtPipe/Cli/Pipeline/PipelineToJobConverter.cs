using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Configuration;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Cli.Pipeline;

public static class PipelineToJobConverter
{
    public static (Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag, Dictionary<string, CliJobContext> Contexts) Convert(
        ParsedPipeline parsed,
        IEnumerable<IStreamTransformerFactory>? streamTransformerFactories = null,
        DtPipe.Cli.Security.ISecretsManager? secretsManager = null,
        IEnumerable<IStreamReaderFactory>? readerFactories = null,
        IEnumerable<IDataWriterFactory>? writerFactories = null,
        IEnumerable<IDataTransformerFactory>? dataTransformerFactories = null)
    {
        // --job mode: load from YAML file and apply CLI overrides
        if (!string.IsNullOrEmpty(parsed.Globals.JobFile))
            return ConvertFromJobFile(parsed, streamTransformerFactories, secretsManager);

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

            // F3 round-trip fidelity: reconstruct transformers and provider options so that
            // CLI → YAML (--export-job) → run (--job) preserves the full pipeline semantics.
            var processor = processorFactories?.FirstOrDefault(f => f.IsApplicable(branchSpec.RawArgs));
            job = job with
            {
                Transformers = BuildTransformerConfigs(branchSpec.PipelineArgs, dataTransformerFactories),
                ProviderOptions = BuildProviderOptions(
                    ForFactoryLookup(job.Input, secretsManager), ForFactoryLookup(job.Output, secretsManager),
                    branchSpec.ReaderArgs, branchSpec.WriterArgs,
                    readerFactories, writerFactories,
                    processor, branchSpec.RawArgs)
            };

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
                ProcessorName = processor?.ComponentName,
                Engine = DeriveEngineSettings(parsed.Globals, branchSpec.Flags)
            });
        }

        var dag = new JobDagDefinition { Branches = branches };
        return (jobs, dag, contexts);
    }

    private static (Dictionary<string, JobDefinition> Jobs, JobDagDefinition Dag, Dictionary<string, CliJobContext> Contexts) ConvertFromJobFile(
        ParsedPipeline parsed,
        IEnumerable<IStreamTransformerFactory>? streamTransformerFactories,
        DtPipe.Cli.Security.ISecretsManager? secretsManager)
    {
        var jobs = JobFileParser.Parse(parsed.Globals.JobFile!, secretsManager);
        var flags = parsed.Globals.AllFlags;

        // Apply CLI overrides to all loaded jobs — driven by EngineOverrideFlags.All
        // (F11 single source), preserving the historical >0/non-empty guard semantics.
        int? limitOverride = GetInt(flags, "--limit");
        int? batchOverride = GetInt(flags, "--batch-size", "-b");
        long? maxBatchBytesOverride = GetLong(flags, "--max-batch-bytes");
        string? logOverride = GetString(flags, "--log");
        string? metricsOverride = GetString(flags, "--metrics-path");
        string? prefixOverride = GetString(flags, "--prefix", "-p");
        string? cursorOverride = GetString(flags, "--cursor");
        string? stateOverride = GetString(flags, "--state");
        double? samplingRateOverride = GetDouble(flags, "--sampling-rate", "--sample-rate");
        int? samplingSeedOverride = GetNullableInt(flags, "--sampling-seed", "--sample-seed");

        foreach (var alias in jobs.Keys.ToList())
        {
            var job = jobs[alias];
            if (parsed.Globals.DryRunCount > 0) job = job with { DryRunCount = parsed.Globals.DryRunCount };
            if (limitOverride is > 0)           job = job with { Limit = limitOverride.Value };
            if (batchOverride is > 0)           job = job with { BatchSize = batchOverride.Value };
            if (maxBatchBytesOverride is > 0)   job = job with { MaxBatchBytes = maxBatchBytesOverride.Value };
            if (!string.IsNullOrEmpty(logOverride))     job = job with { LogPath = logOverride };
            if (!string.IsNullOrEmpty(metricsOverride)) job = job with { MetricsPath = metricsOverride };
            if (!string.IsNullOrEmpty(prefixOverride))  job = job with { Prefix = prefixOverride };
            if (!string.IsNullOrEmpty(cursorOverride))  job = job with { Cursor = cursorOverride };
            if (!string.IsNullOrEmpty(stateOverride))   job = job with { State = stateOverride };
            if (!string.IsNullOrEmpty(parsed.Globals.Session)) job = job with { Session = parsed.Globals.Session };
            if (samplingRateOverride is > 0)    job = job with { SamplingRate = samplingRateOverride.Value };
            if (samplingSeedOverride.HasValue)  job = job with { SamplingSeed = samplingSeedOverride.Value };
            jobs[alias] = job;
        }

        var branches = jobs.Select(kv => new BranchDefinition
        {
            Alias = kv.Key,
            Input = kv.Value.Input,
            Output = kv.Value.Output,
            StreamingAliases = kv.Value.From != null
                ? kv.Value.From.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                : Array.Empty<string>(),
            RefAliases = kv.Value.Ref ?? Array.Empty<string>(),
            Arguments = Array.Empty<string>(),
            ProcessorName = streamTransformerFactories?
                .FirstOrDefault(f => f.IsApplicable(kv.Value))
                ?.ComponentName,
            Engine = new BranchEngineSettings(
                Limit: kv.Value.Limit, BatchSize: kv.Value.BatchSize, MaxBatchBytes: kv.Value.MaxBatchBytes,
                SamplingRate: kv.Value.SamplingRate, SamplingSeed: kv.Value.SamplingSeed,
                DryRunCount: kv.Value.DryRunCount, NoStats: kv.Value.NoStats,
                MetricsPath: kv.Value.MetricsPath, LogPath: kv.Value.LogPath,
                Prefix: kv.Value.Prefix, Cursor: kv.Value.Cursor, State: kv.Value.State,
                Checkpoint: kv.Value.Checkpoint, FromCheckpoint: kv.Value.FromCheckpoint)
        }).ToList();

        return (jobs, new JobDagDefinition { Branches = branches }, new Dictionary<string, CliJobContext>(StringComparer.OrdinalIgnoreCase));
    }

    private static JobDefinition MapToJobDefinition(GlobalOptions globals, BranchSpec branch)
    {
        // F7 single derivation point for engine controls (global defaults overlaid by
        // branch-local flags); provider-level fields stay on the job.
        var engine = DeriveEngineSettings(globals, branch.Flags);
        var job = new JobDefinition
        {
            Input  = branch.Input,
            Output = branch.Output,

            // Global, but carried on every branch: each one resolves its own store, and they
            // must all land in the same session.
            Session = globals.Session,

            // Empty means "this branch has no upstream", which is the absence of the key, not an
            // empty string: string.Join over no aliases wrote "from: ''" on every source branch.
            From     = branch.From.Count > 0 ? string.Join(",", branch.From) : null,
            Ref      = branch.Ref.ToArray(),

            Transformers    = null,
            ProviderOptions = null
        };
        return engine.ApplyTo(job);
    }

    internal static BranchEngineSettings DeriveEngineSettings(GlobalOptions globals, IReadOnlyDictionary<string, List<string>> branchFlags)
    {
        int batchSize = GetInt(branchFlags, "--batch-size", "-b")
                     ?? GetInt(globals.AllFlags, "--batch-size", "-b")
                     ?? PipelineOptions.DefaultBatchSize;
        long maxBatchBytes = GetLong(branchFlags, "--max-batch-bytes")
                          ?? GetLong(globals.AllFlags, "--max-batch-bytes")
                          ?? 0;
        int limit = GetInt(branchFlags, "--limit")
                 ?? GetInt(globals.AllFlags, "--limit")
                 ?? 0;
        double samplingRate = GetDouble(branchFlags, "--sampling-rate", "--sample-rate")
                           ?? GetDouble(globals.AllFlags, "--sampling-rate", "--sample-rate")
                           ?? 1.0;
        int? samplingSeed = GetNullableInt(branchFlags, "--sampling-seed", "--sample-seed")
                         ?? GetNullableInt(globals.AllFlags, "--sampling-seed", "--sample-seed");
        string? logPath = GetString(branchFlags, "--log") ?? globals.LogPath;
        string? metricsPath = GetString(branchFlags, "--metrics-path")
                           ?? GetString(globals.AllFlags, "--metrics-path");
        string? prefix = GetString(branchFlags, "--prefix", "-p")
                      ?? GetString(globals.AllFlags, "--prefix", "-p");
        string? cursor = GetString(branchFlags, "--cursor")
                      ?? GetString(globals.AllFlags, "--cursor");
        string? state = GetString(branchFlags, "--state")
                     ?? GetString(globals.AllFlags, "--state");
        string? checkpoint = GetString(branchFlags, "--checkpoint")
                          ?? GetString(globals.AllFlags, "--checkpoint");
        string? fromCheckpoint = GetString(branchFlags, "--from-checkpoint")
                              ?? GetString(globals.AllFlags, "--from-checkpoint");

        return new BranchEngineSettings(
            Limit: limit,
            BatchSize: batchSize,
            MaxBatchBytes: maxBatchBytes,
            SamplingRate: samplingRate,
            SamplingSeed: samplingSeed,
            DryRunCount: globals.DryRunCount,
            NoStats: globals.NoStats,
            MetricsPath: metricsPath,
            LogPath: logPath,
            Prefix: prefix,
            Cursor: cursor,
            State: state,
            Checkpoint: checkpoint,
            FromCheckpoint: fromCheckpoint);
    }


    // ── F3 round-trip reconstruction helpers ────────────────────────────────

    /// <summary>
    /// Rebuilds the YAML transformer configs from a branch's pipeline args using the same
    /// grouping rule as live execution (consecutive flags of one factory = one step).
    /// Returns null when no transformer args are present or no factories were supplied.
    /// </summary>
    private static List<TransformerConfig>? BuildTransformerConfigs(
        string[] pipelineArgs,
        IEnumerable<IDataTransformerFactory>? dataTransformerFactories)
    {
        if (dataTransformerFactories == null || pipelineArgs is not { Length: > 0 })
            return null;

        var builder = new DtPipe.Cli.Infrastructure.TransformerPipelineBuilder(dataTransformerFactories);
        var configs = new List<TransformerConfig>();
        foreach (var (factory, pairs) in builder.CollectGroups(pipelineArgs))
        {
            var instance = Activator.CreateInstance(factory.OptionsType)!;
            OptionBinder.BindPairs(instance, pairs);
            var config = OptionObjectExporter.ExportTransformerConfig(factory.ComponentName, instance);
            if (config != null)
                configs.Add(config);
        }
        return configs.Count > 0 ? configs : null;
    }

    /// <summary>
    /// Rebuilds provider-options entries from stage-scoped reader/writer args, mirroring
    /// the YAML conventions: plain component key for the reader,
    /// <c>&lt;component&gt;-writer</c> for the writer. Only values that differ from the
    /// options defaults are emitted. A detected stream processor contributes its own
    /// payload under its component name (e.g. <c>sql</c>, <c>merge</c>).
    /// The two connection strings serve only to identify the components; they are never
    /// emitted, so passing a resolved form of an indirection leaks nothing into the job file.
    /// </summary>
    private static Dictionary<string, Dictionary<string, object?>>? BuildProviderOptions(
        string? inputForLookup, string? outputForLookup,
        string[] readerArgs, string[] writerArgs,
        IEnumerable<IStreamReaderFactory>? readerFactories,
        IEnumerable<IDataWriterFactory>? writerFactories,
        IStreamTransformerFactory? processor = null,
        string[]? branchRawArgs = null)
    {
        if (readerFactories == null && writerFactories == null && processor == null)
            return null;

        var result = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase);

        // When a reader and a writer share the same component name (csv, jsonl…), the plain
        // key would be consumed by BOTH at load time — suffix both entries explicitly.
        var readerFactory = ResolveFactory(inputForLookup, readerFactories);
        var readerKey = readerFactory?.ComponentName;
        if (readerFactory != null && writerFactories?.Any(w => w.ComponentName.Equals(readerFactory.ComponentName, StringComparison.OrdinalIgnoreCase)) == true)
            readerKey += "-reader";

        var writerFactory2 = ResolveFactory(outputForLookup, writerFactories);
        var readerEntry = readerFactory != null && readerArgs is { Length: > 0 }
            ? BindToOptionDictionary(readerFactory.OptionsType, readerArgs, readerFactory.ComponentName)
            : null;
        if (readerFactory != null && readerEntry is { Count: > 0 } && readerKey != null)
            result[readerKey] = readerEntry.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value);

        if (writerFactory2 != null && writerArgs is { Length: > 0 })
        {
            var entry = BindToOptionDictionary(writerFactory2.OptionsType, writerArgs, writerFactory2.ComponentName);
            if (entry is { Count: > 0 })
                result[writerFactory2.ComponentName + "-writer"] = entry.ToDictionary(kvp => kvp.Key, kvp => (object?)kvp.Value);
        }

        if (processor != null && branchRawArgs != null)
        {
            var payload = processor.ExportToProviderOptions(branchRawArgs);
            if (payload != null)
                result[processor.ComponentName] = payload;
        }

        return result.Count > 0 ? result : null;
    }

    /// <summary>
    /// Expands a bare <c>keyring://alias</c> so the component behind it can be identified.
    /// An unresolved indirection matches no <see cref="ComponentSelector"/> prefix and no
    /// <c>CanHandle</c>, so <see cref="ResolveFactory"/> returned null and every reader option —
    /// the query included — was dropped from the exported job file, which then died on
    /// "A query is required for provider 'mssql'". <c>JobService.RunSingleJobAsync</c> resolves a
    /// throwaway copy against the same gate before binding; this is the export side of it.
    /// The expansion stays inside factory lookup: the job keeps the keyring reference.
    /// </summary>
    private static string? ForFactoryLookup(string? connectionString, DtPipe.Cli.Security.ISecretsManager? secretsManager)
    {
        const string keyringPrefix = "keyring://";
        if (secretsManager == null || string.IsNullOrWhiteSpace(connectionString))
            return connectionString;

        var raw = connectionString.Trim();
        if (!raw.StartsWith(keyringPrefix, StringComparison.OrdinalIgnoreCase))
            return connectionString;

        return secretsManager.GetSecret(raw[keyringPrefix.Length..].Trim()) ?? connectionString;
    }

    private static T? ResolveFactory<T>(string? connectionString, IEnumerable<T>? factories)
        where T : class, IDataFactory
    {
        if (factories == null || string.IsNullOrEmpty(connectionString))
            return null;

        var raw = connectionString.Trim();
        foreach (var factory in factories)
        {
            if (ComponentSelector.Matches(raw, factory.ComponentName))
                return factory;
        }
        return factories.FirstOrDefault(f => f.CanHandle(raw));
    }

    private static Dictionary<string, string>? BindToOptionDictionary(Type optionsType, string[] args, string prefix)
    {
        object instance;
        try
        {
            instance = Activator.CreateInstance(optionsType)!;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Warning: cannot export provider options for '{prefix}': {ex.Message}");
            return null;
        }

        var registry = new FlagRegistry();
        foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(optionsType))
            registry.Register(def);

        OptionBinder.BindCli(instance, args, registry, prefix);
        return OptionObjectExporter.CollectChanged(instance);
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

    private static long? GetLong(IReadOnlyDictionary<string, List<string>> flags, params string[] keys)
    {
        var s = GetString(flags, keys);
        return s != null && long.TryParse(s, out var v) ? v : null;
    }

    private static long? GetLong(IReadOnlyDictionary<string, object?> flags, params string[] keys)
    {
        var s = GetString(flags, keys);
        return s != null && long.TryParse(s, out var v) ? v : null;
    }
}
