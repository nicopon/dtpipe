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

    public async Task<int> ExecuteAsync(
        JobDefinition job,
        string[] args,
        CancellationToken token,
        System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector = null,
        bool isDag = false,
        string? localAlias = null,
        BranchChannelContext? ctx = null,
        bool showStatusMessages = false)
    {
        var exportService = _serviceProvider.GetRequiredService<ExportService>();
        var currentRawArgs = job.Arguments ?? args;

        if (job.Limit < 0)
            throw new ArgumentException($"--limit value must be >= 0 (got {job.Limit}).");

        // Apply channel routing provided by DagOrchestrator (memory channels between DAG branches)
        if (ctx?.ChannelInjection is { } plan)
        {
            job = job with
            {
                Input = plan.InputChannelAlias != null ? DtPipe.Cli.Helpers.ChannelSpecHelper.ArrowMemory(plan.InputChannelAlias) : job.Input,
                Output = plan.OutputChannelAlias != null ? DtPipe.Cli.Helpers.ChannelSpecHelper.ArrowMemory(plan.OutputChannelAlias) : job.Output,
                NoStats = job.NoStats || plan.SuppressStats
            };
        }

        // 1. Resolve Reader (strips "componentName:" prefix, e.g. "arrow-memory:src" → "src")
        var (readerFactory, cleanedInput) = ResolveFactory<IStreamReaderFactory>(job.Input ?? "", _readerFactories);

        // 2. Resolve Stream Transformer (SQL/Merge) if any
        var streamTransformerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
        var applicableFactory = streamTransformerFactories.FirstOrDefault(f => f.IsApplicable(currentRawArgs));

        if (applicableFactory != null)
        {
            var transformer = applicableFactory.Create(currentRawArgs, ctx ?? new BranchChannelContext(), _serviceProvider);
            readerFactory = new StreamTransformerReaderAdapter(transformer);
            cleanedInput = "";
        }

        if (readerFactory == null)
        {
            if (string.IsNullOrEmpty(cleanedInput))
                throw new InvalidOperationException("No input source specified and no stream transformer detected.");
            throw new InvalidOperationException($"No reader factory resolved for input '{job.Input}'");
        }

        // 3. Resolve Writer (also strips prefix)
        IDataWriterFactory? writerFactory = null;
        string cleanedOutput = job.Output ?? "";
        if (!string.IsNullOrEmpty(job.Output))
        {
            (writerFactory, cleanedOutput) = ResolveFactory<IDataWriterFactory>(job.Output, _writerFactories);
        }

        // 3b. Load query/hook content from files if the value is a file path (e.g. --query my.sql)
        job = job with
        {
            Query = LoadOrReadContent(job.Query, _console, "query"),
            PreExec = LoadOrReadContent(job.PreExec, _console, "Pre-Exec"),
            PostExec = LoadOrReadContent(job.PostExec, _console, "Post-Exec"),
            OnErrorExec = LoadOrReadContent(job.OnErrorExec, _console, "On-Error-Exec"),
            FinallyExec = LoadOrReadContent(job.FinallyExec, _console, "Finally-Exec")
        };

        // 3c. Auto-build query from --table for readers that require a query (e.g. DuckDB, PG, Oracle)
        if (readerFactory.RequiresQuery && string.IsNullOrWhiteSpace(job.Query))
        {
            if (!string.IsNullOrWhiteSpace(job.Table))
                job = job with { Query = $"SELECT * FROM \"{job.Table}\"" };
        }

        // 4. Global Options (from job definition)
        var pipelineOptions = new PipelineOptions
        {
            Key = job.Key,
            LogPath = job.LogPath,
            MetricsPath = job.MetricsPath,
            Strategy = job.Strategy,
            InsertMode = job.InsertMode,
            Limit = job.Limit,
            SamplingRate = job.SamplingRate,
            SamplingSeed = job.SamplingSeed,
            BatchSize = job.BatchSize,
            ConnectionTimeout = job.ConnectionTimeout,
            QueryTimeout = job.QueryTimeout,
            UnsafeQuery = job.UnsafeQuery,
            DryRunCount = job.DryRunCount,
            StrictSchema = job.StrictSchema,
            NoSchemaValidation = job.NoSchemaValidation,
            AutoMigrate = job.AutoMigrate ?? false,
            PreExec = job.PreExec,
            PostExec = job.PostExec,
            OnErrorExec = job.OnErrorExec,
            FinallyExec = job.FinallyExec,
            Path = job.Path,
            ColumnTypes = job.ColumnTypes,
            AutoColumnTypes = job.AutoColumnTypes,
            MaxSample = job.MaxSample,
            Encoding = job.Encoding,
            Table = job.Table,
            NoStats = job.NoStats,
            SchemaSave = job.SchemaSave,
            SchemaLoad = job.SchemaLoad,
            OutputPath = cleanedOutput,
            ConnectionString = cleanedInput,
            Query = job.Query
        };

        // 5. Build Pipeline (Transformers)
        // For CLI-originated branches (have raw args), always use TransformerPipelineBuilder
        // which calls CreateFromConfiguration — the correct format for CLI flags.
        // BuildPipelineFromYaml is reserved for YAML-only jobs (no raw args) because
        // TransformerConfig.Mappings format doesn't match CLI syntax for expression transformers (--filter).
        var tFactories = _contributors.OfType<IDataTransformerFactory>().ToList();
        List<IDataTransformer> pipeline;

        if (currentRawArgs.Length > 0)
        {
            var pipelineBuilder = new TransformerPipelineBuilder(tFactories);
            pipeline = pipelineBuilder.Build(currentRawArgs);
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
                    
                    (writerFactory, _) = ResolveFactory<IDataWriterFactory>("null:", _writerFactories);
                }

                if (writerFactory == null)
                {
                    throw new InvalidOperationException($"No writer factory resolved for output '{job.Output ?? "null:"}'");
                }
            }

            await exportService.RunExportAsync(pipelineOptions, readerFactory.ComponentName, cleanedOutput, token, pipeline, readerFactory, writerFactory, _optionsRegistry, isDag ? localAlias : null, resultsCollector, showStatusMessages);
            return 0;
        }
        catch (OperationCanceledException)
        {
            return 0;
        }
        catch (Exception ex)
        {
            _console.Write(new Spectre.Console.Markup($"{Environment.NewLine}[red]Error: {Markup.Escape(ex.Message)}[/]{Environment.NewLine}"));
            if (Environment.GetEnvironmentVariable("DEBUG") == "1")
                _console.WriteLine(ex.StackTrace ?? "");
            return 1;
        }
    }

    private static (T? Factory, string Cleaned) ResolveFactory<T>(string raw, IEnumerable<T> factories) where T : class, IDataFactory
    {
        raw = raw.Trim();
        foreach (var factory in factories)
        {
            var prefix = factory.ComponentName + ":";
            if (raw.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return (factory, raw[prefix.Length..].Trim());
            // Bare component name (e.g. "-o csv") → maps to stdio "-"
            if (string.Equals(raw, factory.ComponentName, StringComparison.OrdinalIgnoreCase))
                return (factory, "-");
        }
        var match = factories.FirstOrDefault(f => f.CanHandle(raw));
        return (match, raw);
    }

    private static string? LoadOrReadContent(string? input, IAnsiConsole console, string label)
    {
        if (string.IsNullOrWhiteSpace(input)) return input;
        input = input.Trim();

        string? filename = null;
        if (input.StartsWith("@"))
            filename = input.Substring(1).Trim();
        else if (File.Exists(input) &&
                 (input.EndsWith(".sql", StringComparison.OrdinalIgnoreCase) ||
                  input.EndsWith(".txt", StringComparison.OrdinalIgnoreCase) ||
                  input.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) ||
                  input.EndsWith(".yml", StringComparison.OrdinalIgnoreCase)))
            filename = input;

        if (filename != null)
        {
            if (File.Exists(filename))
            {
                console.WriteLine($"Loaded {label} from file: {filename}");
                return File.ReadAllText(filename).Trim();
            }
            else if (input.StartsWith("@"))
                throw new InvalidOperationException($"File not found for {label}: {filename}");
        }
        return input;
    }

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

internal class StreamTransformerReaderAdapter : IStreamReaderFactory
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
        public IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, CancellationToken ct = default) 
            => throw new NotSupportedException("StreamTransformerReaderAdapter only supports columnar mode.");
        public ValueTask DisposeAsync() => _transformer.DisposeAsync();
    }
}
