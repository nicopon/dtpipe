using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Core.Pipelines;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Services;

public class LinearPipelineService
{
    private readonly IEnumerable<ICliContributor> _contributors;
    private readonly IServiceProvider _serviceProvider;
    private readonly IMemoryChannelRegistry _channelRegistry;
    private readonly OptionsRegistry _optionsRegistry;
    private readonly IAnsiConsole _console;
    private readonly IEnumerable<IDataWriterFactory> _writerFactories;
    private readonly IEnumerable<IStreamReaderFactory> _readerFactories;

    // Cancellation sources for user-vs-internal discrimination (F16):
    // user-initiated shutdown reports exit code 130; internal cancellation propagates.
    private CancellationToken _userCancellationToken;
    private CancellationTokenSource? _internalCts;

    public LinearPipelineService(
        IEnumerable<ICliContributor> contributors,
        IServiceProvider serviceProvider,
        IMemoryChannelRegistry channelRegistry,
        OptionsRegistry optionsRegistry,
        IAnsiConsole console)
    {
        _contributors = contributors;
        _serviceProvider = serviceProvider;
        _channelRegistry = channelRegistry;
        _optionsRegistry = optionsRegistry;
        _console = console;
        _writerFactories = _serviceProvider.GetRequiredService<IEnumerable<IDataWriterFactory>>();
        _readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
    }

    public Task<int> ExecuteAsync(
        JobDefinition job,
        CliJobContext? context,
        CancellationToken token,
        System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector = null,
        bool isDag = false,
        string? localAlias = null,
        BranchChannelContext? ctx = null,
        bool showStatusMessages = false,
        string? dryRunInteractiveBranch = null,
        bool quiet = false)
        => ExecuteAsync(job, context, token, CancellationToken.None, resultsCollector, isDag, localAlias, ctx, showStatusMessages, dryRunInteractiveBranch, quiet);

    public async Task<int> ExecuteAsync(
        JobDefinition job,
        CliJobContext? context,
        CancellationToken token,
        CancellationToken userCancellationToken,
        System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector = null,
        bool isDag = false,
        string? localAlias = null,
        BranchChannelContext? ctx = null,
        bool showStatusMessages = false,
        string? dryRunInteractiveBranch = null,
        bool quiet = false)
    {
        // Internal cancellation models non-user sources (DAG branch faults, host teardown);
        // the working token honors both so a Ctrl-C still stops the pipeline cooperatively.
        _userCancellationToken = userCancellationToken;
        using var internalCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        _internalCts = internalCts;
        using var workCts = CancellationTokenSource.CreateLinkedTokenSource(internalCts.Token, userCancellationToken);
        var workCt = workCts.Token;
        return await ExecuteCoreAsync(job, context, workCt, resultsCollector, isDag, localAlias, ctx, showStatusMessages, dryRunInteractiveBranch, quiet);
    }

    private async Task<int> ExecuteCoreAsync(
        JobDefinition job,
        CliJobContext? context,
        CancellationToken token,
        System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector,
        bool isDag,
        string? localAlias,
        BranchChannelContext? ctx,
        bool showStatusMessages,
        string? dryRunInteractiveBranch,
        bool quiet)
    {
        var exportService = _serviceProvider.GetRequiredService<ExportService>();
        var currentRawArgs = context?.Arguments ?? System.Array.Empty<string>();

        if (job.Limit < 0)
            throw new ArgumentException($"--limit value must be >= 0 (got {job.Limit}).");

        // F5: consume the orchestrator's typed channel endpoints directly — no CLI flag
        // syntax is synthesized; reader/writer factories are selected by capability.
        InternalChannelEndpoint? inputEndpoint = ctx?.InputEndpoint;
        InternalChannelEndpoint? outputEndpoint = ctx?.OutputEndpoint;
        if (ctx?.SuppressStats == true && !job.NoStats)
            job = job with { NoStats = true };

        // Resolve keyring connection string secrets
        var resolver = _serviceProvider.GetService<DtPipe.Core.Expressions.IStringContentResolver>();
        if (resolver != null)
        {
            if (job.Input != null)
            {
                var resolvedInput = await resolver.ResolveAsync(job.Input, token);
                if (resolvedInput != null) job = job with { Input = resolvedInput };
            }
            if (job.Output != null)
            {
                var resolvedOutput = await resolver.ResolveAsync(job.Output, token);
                if (resolvedOutput != null) job = job with { Output = resolvedOutput };
            }
        }

        // 1. Resolve Reader. A typed input endpoint bypasses connection-string resolution
        // entirely: the factory is picked by capability and the channel alias handed over.
        // --from-checkpoint resolves like a typed endpoint, by capability: a checkpoint key is
        // not a connection string and must not enter the ComponentSelector grammar, where a
        // hex key could be read as a component prefix.
        (IStreamReaderFactory? readerFactory, string cleanedInput, string? inputVariant) =
            !string.IsNullOrEmpty(job.FromCheckpoint)
                ? (new DtPipe.Sessions.CheckpointReaderFactory(job.FromCheckpoint, job.Session), job.FromCheckpoint, (string?)null)
            : inputEndpoint != null
                ? (PickChannelReader(inputEndpoint.Kind), inputEndpoint.Alias, (string?)null)
                : ResolveFactory<IStreamReaderFactory>(job.Input ?? "", _readerFactories);

        // 2. Resolve Stream Transformer (SQL / Merge / …): CLI args when present, else the YAML JobDefinition.
        //    Each factory owns both surfaces (Create for CLI tokens, CreateFromJob for provider-options);
        //    this dispatcher stays free of any processor-specific knowledge.
        var streamTransformerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();

        IStreamTransformer? streamTransformer = null;
        if (currentRawArgs.Length > 0)
        {
            var cliFactory = streamTransformerFactories.FirstOrDefault(f => f.IsApplicable(currentRawArgs));
            if (cliFactory != null)
                streamTransformer = cliFactory.Create(currentRawArgs, ctx ?? new BranchChannelContext(), _serviceProvider);
        }
        else
        {
            var yamlFactory = streamTransformerFactories.FirstOrDefault(f => f.IsApplicable(job));
            if (yamlFactory != null)
                streamTransformer = yamlFactory.CreateFromJob(job, ctx ?? new BranchChannelContext(), _serviceProvider);
        }

        if (streamTransformer != null)
        {
            readerFactory = new StreamTransformerReaderAdapter(streamTransformer);
            cleanedInput = "";
        }

        if (readerFactory == null)
        {
            if (string.IsNullOrEmpty(cleanedInput))
                throw new InvalidOperationException("No input source specified and no stream transformer detected. When combining multiple branches ('from'/'ref'), specify a stream transformer query under 'provider-options -> sql -> query: \"SELECT ... FROM branch1 JOIN branch2 ON ...\"'.");
            throw new InvalidOperationException($"No reader factory resolved for input '{job.Input}'");
        }

        // 3. Resolve Writer — typed output endpoint first (capability-selected), then
        // connection-string resolution for explicit -o targets.
        IDataWriterFactory? writerFactory = null;
        string cleanedOutput;
        string? outputVariant = null;
        if (outputEndpoint != null)
        {
            writerFactory = PickChannelWriter(outputEndpoint.Kind);
            cleanedOutput = outputEndpoint.Alias;
        }
        else
        {
            cleanedOutput = job.Output ?? "";
            if (!string.IsNullOrEmpty(job.Output))
            {
                (writerFactory, cleanedOutput, outputVariant) = ResolveFactory<IDataWriterFactory>(job.Output, _writerFactories);
            }
        }

        // Universal pipeline options (engine controls only; adapter options are in the registry).
        // Registered BEFORE any factory-options probe below: stream-transformer branches alias
        // their factory OptionsType to PipelineOptions, so an early Get would miss and warn.
        var pipelineOptions = new PipelineOptions
        {
            MetricsPath  = job.MetricsPath,
            Limit        = job.Limit,
            SamplingRate = job.SamplingRate,
            SamplingSeed = job.SamplingSeed,
            BatchSize    = job.BatchSize,
            MaxBatchBytes = job.MaxBatchBytes,
            DryRunCount  = job.DryRunCount,
            NoStats      = job.NoStats,
            DryRunInteractiveBranch = dryRunInteractiveBranch,
            Cursor       = job.Cursor,
            State        = job.State,
            Checkpoint   = job.Checkpoint,
            FromCheckpoint = job.FromCheckpoint,
            Session      = job.Session
        };
        _optionsRegistry.Register(pipelineOptions);

        // 3b. Query file resolution: OptionBinder sets readerOpts.Query
        // from --query flag or YAML — resolve file refs here.
        // This is separate from 3b because job.Query is null for CLI branches.
        {
            DtPipe.Core.Options.IQueryAwareOptions? readerOptsForLoad = null;
            if (_optionsRegistry.TryGetByType(readerFactory.OptionsType, out var readerOptsRaw))
                readerOptsForLoad = readerOptsRaw as DtPipe.Core.Options.IQueryAwareOptions;
            if (readerOptsForLoad != null && !string.IsNullOrWhiteSpace(readerOptsForLoad.Query))
            {
                var resolved = await (resolver ?? DtPipe.Core.Expressions.DefaultStringContentResolver.Instance).ResolveAsync(readerOptsForLoad.Query, token);
                if (resolved != readerOptsForLoad.Query)
                    readerOptsForLoad.Query = resolved;
            }
        }

        // 3c. Load hook content from files for adapter options (CLI path: already bound by OptionBinder)
        if (writerFactory != null)
        {
            var writerHookOpts = _optionsRegistry.Get(writerFactory.OptionsType) as DtPipe.Core.Options.IHookAware;
            if (writerHookOpts != null)
            {
                if (!string.IsNullOrEmpty(writerHookOpts.PreExec))
                    writerHookOpts.PreExec = await (resolver ?? DtPipe.Core.Expressions.DefaultStringContentResolver.Instance).ResolveAsync(writerHookOpts.PreExec, token);
                if (!string.IsNullOrEmpty(writerHookOpts.PostExec))
                    writerHookOpts.PostExec = await (resolver ?? DtPipe.Core.Expressions.DefaultStringContentResolver.Instance).ResolveAsync(writerHookOpts.PostExec, token);
                if (!string.IsNullOrEmpty(writerHookOpts.OnErrorExec))
                    writerHookOpts.OnErrorExec = await (resolver ?? DtPipe.Core.Expressions.DefaultStringContentResolver.Instance).ResolveAsync(writerHookOpts.OnErrorExec, token);
                if (!string.IsNullOrEmpty(writerHookOpts.FinallyExec))
                    writerHookOpts.FinallyExec = await (resolver ?? DtPipe.Core.Expressions.DefaultStringContentResolver.Instance).ResolveAsync(writerHookOpts.FinallyExec, token);
            }
        }

        // 3d. RequiresQuery auto-build: if the reader needs a SQL query and none was provided,
        // check (in order): reader's own --table, writer's --table, YAML job.Query.
        if (readerFactory.RequiresQuery)
        {
            DtPipe.Core.Options.IQueryAwareOptions? readerOpts = null;
            if (_optionsRegistry.TryGetByType(readerFactory.OptionsType, out var readerOptsObj))
                readerOpts = readerOptsObj as DtPipe.Core.Options.IQueryAwareOptions;
            if (readerOpts != null && string.IsNullOrWhiteSpace(readerOpts.Query))
            {
                // 1. Reader's own --table (e.g. DuckDB reader: --table source_table)
                var readerTable = (readerOpts as ITableAwareOptions)?.Table;
                if (!string.IsNullOrWhiteSpace(readerTable))
                {
                    readerOpts.Query = $"SELECT * FROM \"{readerTable}\"";
                }
                else
                {
                    // 2. Writer's --table (same-name read/write)
                    object? writerOpts = null;
                    if (writerFactory != null)
                        _optionsRegistry.TryGetByType(writerFactory.OptionsType, out writerOpts);
                    var tableVal = (writerOpts as ITableAwareOptions)?.Table;
                    if (!string.IsNullOrWhiteSpace(tableVal))
                        readerOpts.Query = $"SELECT * FROM \"{tableVal}\"";
                }
            }
        }

        // 4. Register routing so factory Create() methods can resolve adapter connection strings.
        _optionsRegistry.Register(new DtPipe.Cli.Infrastructure.ConnectionRoute(cleanedInput, cleanedOutput, inputVariant, outputVariant));

        // 5. Build Pipeline (Transformers)
        // For CLI-originated branches (have raw args), always use TransformerPipelineBuilder
        // which calls CreateFromConfiguration — the correct format for CLI flags.
        // BuildPipelineFromYaml is reserved for YAML-only jobs (no raw args) because
        // TransformerConfig.Mappings format doesn't match CLI syntax for expression transformers (--filter).
        var tFactories = _contributors.OfType<IDataTransformerFactory>().ToList();
        List<IDataTransformer> pipeline;

        // Use PipelineArguments (transformer scope only) when available; fall back to full RawArgs.
        var transformerArgs = context?.PipelineArguments is { Length: > 0 }
            ? context.PipelineArguments
            : currentRawArgs;

        if (transformerArgs.Length > 0)
        {
            var pipelineBuilder = new TransformerPipelineBuilder(tFactories);
            pipeline = pipelineBuilder.Build(transformerArgs);
        }
        else if (job.Transformers != null && job.Transformers.Count > 0)
        {
            pipeline = BuildPipelineFromYaml(job, tFactories, _console);
        }
        else
        {
            pipeline = new List<IDataTransformer>();
        }

        try
        {
            // 6. Final Export Execution
            if (writerFactory == null)
            {
                if (string.IsNullOrEmpty(job.Output))
                {
                    if (isDag)
                    {
                        if (pipelineOptions.DryRunCount <= 0)
                            return 0; // Upstream branch to memory channel handled elsewhere
                    }
                    else
                    {
                        // Linear job with no output — validation mode only
                        _console.Write(new Spectre.Console.Markup($"[yellow]Warning: No output specified. Running in validation mode.[/]{Environment.NewLine}"));
                    }

                    (writerFactory, _, _) = ResolveFactory<IDataWriterFactory>("null:", _writerFactories);
                }

                if (writerFactory == null)
                {
                    throw new InvalidOperationException($"No writer factory resolved for output '{job.Output ?? "null:"}'");
                }
            }

            await exportService.RunExportAsync(pipelineOptions, readerFactory.ComponentName, cleanedOutput, token, pipeline, readerFactory, writerFactory, _optionsRegistry, isDag ? localAlias : null, resultsCollector, showStatusMessages, quiet);
            return 0;
        }
        catch (OperationCanceledException)
        {
            // F16: cancellation must not mask as success. User-initiated shutdown
            // (Ctrl-C) reports the POSIX SIGINT convention; internal cancellation
            // propagates to the caller (DAG orchestrator / host) for correct reporting.
            if (_userCancellationToken.IsCancellationRequested
                && _internalCts is { Token.IsCancellationRequested: false })
            {
                _console.Write(new Spectre.Console.Markup($"{Environment.NewLine}[yellow]Warning: Pipeline canceled by user (Ctrl-C).[/]{Environment.NewLine}"));
                return 130;
            }
            throw;
        }
        catch (Exception ex)
        {
            var chain = DtPipe.Core.Infrastructure.Diagnostics.ExceptionChainFlattener.Format(ex);
            _console.Write(new Spectre.Console.Markup($"{Environment.NewLine}[red]Error: {Markup.Escape(chain)}[/]{Environment.NewLine}"));
            if (Environment.GetEnvironmentVariable("DEBUG") == "1")
                _console.WriteLine(ex.StackTrace ?? "");
            return 1;
        }
    }

    /// <summary>
    /// Routes a connection string to its provider. The "{component}[+{variant}]:" grammar — including
    /// the rule that a remote URI is not a selector — belongs to <see cref="ComponentSelector"/>, so
    /// every routing site in the CLI resolves identically.
    /// </summary>
    private static (T? Factory, string Cleaned, string? Variant) ResolveFactory<T>(string raw, IEnumerable<T> factories) where T : class, IDataFactory
    {
        raw = raw.Trim();
        foreach (var factory in factories)
        {
            var selection = ComponentSelector.Select(raw, factory.ComponentName);
            if (selection.Matched)
                return (factory, selection.Cleaned, selection.Variant);
        }
        var match = factories.FirstOrDefault(f => f.CanHandle(raw));
        return (match, raw, null);
    }

    // F5 — capability-based selection of internal channel transports. DI wraps
    // descriptors in CliProviderFactory, which forwards the descriptor's capability.
    private IStreamReaderFactory? PickChannelReader(InternalChannelKind kind)
        => _readerFactories.FirstOrDefault(f => f.CapabilityKind == kind);

    private IDataWriterFactory? PickChannelWriter(InternalChannelKind kind)
        => _writerFactories.FirstOrDefault(f => f.CapabilityKind == kind);


    private List<IDataTransformer> BuildPipelineFromYaml(JobDefinition job, List<IDataTransformerFactory> factories, IAnsiConsole console)
    {
        var pipeline = new List<IDataTransformer>();
        var configs = job.Transformers ?? new List<TransformerConfig>();
        foreach (var config in configs)
        {
            var factory = factories.FirstOrDefault(f => f.ComponentName.Equals(config.Type, StringComparison.OrdinalIgnoreCase));
            if (factory == null) throw new InvalidOperationException($"Transformer factory '{config.Type}' not found.");
            var transformer = factory.CreateFromYamlConfig(config);
            if (transformer != null) pipeline.Add(transformer);
        }

        if (Environment.GetEnvironmentVariable("DEBUG") == "1")
        {
            Console.Error.WriteLine($"[DEBUG] BuildPipelineFromYaml count: {pipeline.Count}");
            foreach (var t in pipeline) Console.Error.WriteLine($"[DEBUG] Transformer: {t.GetType().Name}");
        }

        return pipeline;
    }
}

internal class StreamTransformerReaderAdapter : IStreamReaderFactory, IStreamProcessorSource
{
    private readonly IStreamTransformer _transformer;

    public StreamTransformerReaderAdapter(IStreamTransformer transformer)
    {
        _transformer = transformer;
    }

    public string ComponentName => "stream-adapter";
    public string Category => "Processors";
    public bool CanHandle(string connectionString) => false;
    public Type OptionsType => typeof(PipelineOptions);
    public bool RequiresQuery => false;
    public bool YieldsColumnarOutput => true;

    public IStreamReader Create(OptionsRegistry registry) => new WrappedStreamReader(_transformer);
    public IEnumerable<Type> GetSupportedOptionTypes() => Enumerable.Empty<Type>();

    private class WrappedStreamReader : IColumnarStreamReader
    {
        private readonly IStreamTransformer _transformer;
        public WrappedStreamReader(IStreamTransformer transformer) => _transformer = transformer;
        public IReadOnlyList<PipeColumnInfo>? Columns => _transformer.Columns;
        public Schema? Schema => _transformer.Schema;
        public Task OpenAsync(CancellationToken ct = default) => _transformer.OpenAsync(ct);
        public IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(CancellationToken ct = default) => _transformer.ReadResultsAsync(null, ct);
        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var batch in ReadRecordBatchesAsync(ct))
            {
                foreach (var memory in DtPipe.Core.Infrastructure.Arrow.ArrowRowConverter.FlattenBatch(batch, batchSize))
                {
                    yield return memory;
                }
            }
        }
        public ValueTask DisposeAsync() => _transformer.DisposeAsync();
    }
}
