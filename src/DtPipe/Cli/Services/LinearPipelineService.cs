using System.CommandLine;
using Apache.Arrow;
using DtPipe.Cli.Infrastructure;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Validation;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Services;

/// <summary>
/// Responsible for orchestrating linear (single-branch) pipeline execution.
/// </summary>
public class LinearPipelineService
{
    private readonly IEnumerable<ICliContributor> _contributors;
    private readonly IServiceProvider _serviceProvider;
    private readonly OptionsRegistry _registry;
    private readonly IAnsiConsole _console;

    public LinearPipelineService(
        IEnumerable<ICliContributor> contributors,
        IServiceProvider serviceProvider,
        OptionsRegistry registry,
        IAnsiConsole console)
    {
        _contributors = contributors;
        _serviceProvider = serviceProvider;
        _registry = registry;
        _console = console;
    }

    public async Task<int> ExecuteAsync(
        JobDefinition job,
        string[] currentRawArgs,
        CancellationToken token,
        string? localAlias,
        bool isDag,
        BranchChannelContext? ctx = null,
        System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector = null,
        bool showStatusMessages = false)
    {
        var exportService = _serviceProvider.GetRequiredService<ExportService>();

        // Consume channel routing provided by DagOrchestrator
        if (ctx?.ChannelInjection is { } plan)
        {
            job = job with {
                Input   = plan.InputChannel.HasValue  ? ToChannelSpec(plan.InputChannel.Value)  : job.Input,
                Output  = plan.OutputChannel.HasValue ? ToChannelSpec(plan.OutputChannel.Value) : job.Output,
                NoStats = job.NoStats || plan.SuppressStats
            };
        }

        IStreamReaderFactory readerFactory;
        string cleanedInput;

        // Detect stream transformers (SQL/merge) from branch args
        var streamTransformerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
        var applicableFactory = streamTransformerFactories.FirstOrDefault(f => f.IsApplicable(currentRawArgs));

        if (applicableFactory != null)
        {
            var transformer = applicableFactory.Create(currentRawArgs, ctx ?? new BranchChannelContext(), _serviceProvider);
            readerFactory = new StreamTransformerReaderAdapter(transformer);
            cleanedInput = "";
        }
        else
        {
            var readerFactories = _contributors.OfType<IStreamReaderFactory>().ToList();
            (readerFactory, cleanedInput) = ResolveFactory(readerFactories, job.Input ?? "", "reader");
        }

        job = job with { Input = cleanedInput };

        if (showStatusMessages) _console.WriteLine($"Auto-detected input source: {readerFactory.ComponentName}");

        var writerFactories = _contributors.OfType<IDataWriterFactory>().ToList();

        IDataWriterFactory? writerFactory = null;
        string? cleanedOutput = null;

        if (!isDag || !string.IsNullOrWhiteSpace(job.Output))
        {
            if (!string.IsNullOrWhiteSpace(job.Output))
            {
                (writerFactory, cleanedOutput) = ResolveFactory(writerFactories, job.Output, "writer");
                job = job with { Output = cleanedOutput };
            }
        }

        job = job with
        {
            Query = LoadOrReadContent(job.Query, _console, "query"),
            PreExec = LoadOrReadContent(job.PreExec, _console, "Pre-Exec"),
            PostExec = LoadOrReadContent(job.PostExec, _console, "Post-Exec"),
            OnErrorExec = LoadOrReadContent(job.OnErrorExec, _console, "On-Error-Exec"),
            FinallyExec = LoadOrReadContent(job.FinallyExec, _console, "Finally-Exec")
        };

        if (readerFactory.RequiresQuery)
        {
            if (string.IsNullOrWhiteSpace(job.Query))
            {
                if (!string.IsNullOrWhiteSpace(job.Table))
                {
                    job = job with { Query = $"SELECT * FROM {job.Table}" };
                }
                else
                {
                    _console.Write(new Spectre.Console.Markup($"[red]Error: A query is required for provider '{readerFactory.ComponentName}'. Use --query \"SELECT...\"[/]{Environment.NewLine}"));
                    return 1;
                }
            }

            try { SqlQueryValidator.Validate(job.Query, job.UnsafeQuery); }
            catch (InvalidOperationException ex)
            {
                _console.Write(new Spectre.Console.Markup($"[red]Error: {ex.Message}[/]{Environment.NewLine}"));
                return 1;
            }
        }

        var pipelineOptions = new PipelineOptions
        {
            Provider = readerFactory.ComponentName,
            ConnectionString = job.Input,
            Query = job.Query,
            OutputPath = job.Output ?? "",
            ConnectionTimeout = job.ConnectionTimeout,
            QueryTimeout = job.QueryTimeout,
            BatchSize = job.BatchSize,
            UnsafeQuery = job.UnsafeQuery,
            DryRunCount = RawJobBuilder.ParseDryRunFromArgs(currentRawArgs),
            Limit = job.Limit,
            SamplingRate = job.SamplingRate,
            SamplingSeed = job.SamplingSeed,
            LogPath = job.LogPath,
            Key = job.Key,
            PreExec = job.PreExec,
            PostExec = job.PostExec,
            OnErrorExec = job.OnErrorExec,
            FinallyExec = job.FinallyExec,
            Strategy = job.Strategy,
            InsertMode = job.InsertMode,
            Table = job.Table,
            MaxRetries = job.MaxRetries,
            RetryDelayMs = job.RetryDelayMs,
            StrictSchema = job.StrictSchema,
            NoSchemaValidation = job.NoSchemaValidation,
            NoStats = job.NoStats,
            MetricsPath = job.MetricsPath,
            AutoMigrate = job.AutoMigrate ?? false
        };

        _registry.Register(pipelineOptions);

        var tFactories = _contributors.OfType<IDataTransformerFactory>().ToList();
        List<IDataTransformer> pipeline;

        if (job.Transformers != null && job.Transformers.Count > 0)
        {
            pipeline = BuildPipelineFromYaml(job.Transformers, tFactories, _console);
        }
        else
        {
            var pipelineBuilder = new TransformerPipelineBuilder(tFactories);
            pipeline = pipelineBuilder.Build(currentRawArgs);
        }

        try
        {
            // 6. Final Export Execution
            if (writerFactory == null)
            {
                if (isDag || string.IsNullOrWhiteSpace(job.Output))
                {
                    if (isDag) return 0; // Upstream branch to memory channel handled elsewhere

                    // Linear job - just validation/dry run
                    _console.Write(new Spectre.Console.Markup($"[yellow]Warning: No output specified. Running in validation mode.[/]{Environment.NewLine}"));
                    return 0;
                }
                throw new InvalidOperationException($"No writer factory resolved for output '{job.Output}'");
            }

            if (string.IsNullOrEmpty(job.Output)) throw new InvalidOperationException("No output destination specified.");
            await exportService.RunExportAsync(pipelineOptions, readerFactory.ComponentName, job.Output, token, pipeline, readerFactory, writerFactory, _registry, isDag ? localAlias : null, resultsCollector, showStatusMessages);
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

    private static string ToChannelSpec((DtPipe.Core.Abstractions.Dag.ChannelMode Mode, string Alias) channel)
        => $"{(channel.Mode == DtPipe.Core.Abstractions.Dag.ChannelMode.Arrow ? "arrow-memory" : "mem")}:{channel.Alias}";

    private static (T Factory, string CleanedString) ResolveFactory<T>(IEnumerable<T> factories, string rawString, string typeName) where T : IDataFactory
    {
        rawString = rawString.Trim();

        foreach (var factory in factories)
        {
            var prefix = factory.ComponentName + ":";
            if (rawString.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                var cleaned = rawString.Substring(prefix.Length).Trim();
                if (cleaned == "-" && !factory.SupportsStdio)
                    throw new InvalidOperationException($"The provider '{factory.ComponentName}' does not support standard input/output pipes (-).");
                return (factory, cleaned);
            }

            if (rawString.Equals(factory.ComponentName, StringComparison.OrdinalIgnoreCase))
            {
                if (typeName == "branch")
                    return (factory, "");
                return (factory, "-");
            }
        }

        var matchingFactories = factories.Where(f => f.CanHandle(rawString)).ToList();
        if (matchingFactories.Count == 1)
        {
            var factory = matchingFactories[0];
            if (rawString == "-" && !factory.SupportsStdio)
                throw new InvalidOperationException($"The provider '{factory.ComponentName}' does not support standard input/output pipes (-).");
            return (factory, rawString);
        }

        if (matchingFactories.Count > 1)
        {
            var names = string.Join(", ", matchingFactories.Select(f => f.ComponentName));
            throw new InvalidOperationException($"Ambiguous {typeName} string '{rawString}'. Matches: {names}. Use explicit prefix (e.g. '{matchingFactories[0].ComponentName}:...') to disambiguate.");
        }

        if (matchingFactories.Count == 0 && typeName == "writer" && !rawString.Contains(":") && (rawString.EndsWith("/") || rawString.EndsWith("\\") || !rawString.Contains(".")))
        {
            var parquet = factories.FirstOrDefault(f => f.ComponentName.Equals("parquet", StringComparison.OrdinalIgnoreCase));
            if (parquet != null) return (parquet, rawString);
        }

        throw new InvalidOperationException($"No compatible {typeName} found for connection string '{rawString}'.");
    }

    private static string? LoadOrReadContent(string? input, Spectre.Console.IAnsiConsole console, string label)
    {
        if (string.IsNullOrWhiteSpace(input)) return input;
        input = input.Trim();

        string? filename = null;
        if (input.StartsWith("@"))
            filename = input.Substring(1).Trim();
        else if (File.Exists(input) && (input.EndsWith(".sql", StringComparison.OrdinalIgnoreCase) || input.EndsWith(".txt", StringComparison.OrdinalIgnoreCase) || input.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) || input.EndsWith(".yml", StringComparison.OrdinalIgnoreCase)))
            filename = input;

        if (filename != null)
        {
            if (File.Exists(filename))
            {
                console.WriteLine($"Loaded {label} from File: {filename}");
                return File.ReadAllText(filename).Trim();
            }
            else if (input.StartsWith("@"))
                throw new InvalidOperationException($"File not found for {label}: {filename}");
        }
        return input;
    }

    private static List<IDataTransformer> BuildPipelineFromYaml(
        List<TransformerConfig> configs,
        List<IDataTransformerFactory> factories,
        IAnsiConsole console)
    {
        var pipeline = new List<IDataTransformer>();
        foreach (var config in configs)
        {
            var factory = factories.FirstOrDefault(f =>
                f.ComponentName.Equals(config.Type, StringComparison.OrdinalIgnoreCase));

            if (factory == null)
            {
                console.Write(new Markup($"[yellow]Warning: Unknown transformer type '{config.Type}' in job file. Skipping.[/]{Environment.NewLine}"));
                continue;
            }

            var transformer = factory.CreateFromYamlConfig(config);
            if (transformer != null)
                pipeline.Add(transformer);
        }
        return pipeline;
    }

    /// <summary>
    /// Wraps an <see cref="IStreamTransformer"/> as an <see cref="IStreamReaderFactory"/> so that
    /// <see cref="ExportService"/> can treat it like a regular reader.
    /// </summary>
    private sealed class StreamTransformerReaderAdapter : IStreamReaderFactory
    {
        private readonly IStreamTransformer _transformer;

        public StreamTransformerReaderAdapter(IStreamTransformer transformer)
            => _transformer = transformer;

        public string ComponentName => "stream-transformer";
        public string Category => "Stream Transformers";
        public Type OptionsType => typeof(object);
        public bool SupportsStdio => false;
        public bool RequiresQuery => false;
        public bool YieldsColumnarOutput => true;
        public bool CanHandle(string s) => false;

        public IStreamReader Create(OptionsRegistry registry)
            => new StreamTransformerReader(_transformer);

        public IEnumerable<Type> GetSupportedOptionTypes() => System.Array.Empty<Type>();
    }

    private sealed class StreamTransformerReader : IColumnarStreamReader
    {
        private readonly IStreamTransformer _transformer;

        public StreamTransformerReader(IStreamTransformer transformer)
            => _transformer = transformer;

        public IReadOnlyList<PipeColumnInfo>? Columns => _transformer.Columns;
        public Apache.Arrow.Schema? Schema => null;

        public Task OpenAsync(CancellationToken ct) => _transformer.OpenAsync(ct);

        public IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadRecordBatchesAsync(CancellationToken ct)
            => _transformer.ReadResultsAsync(null, ct);

        public IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
            int batchSize,
            CancellationToken ct)
            => throw new NotSupportedException("StreamTransformerReader only supports columnar (Arrow) mode.");

        public ValueTask DisposeAsync() => _transformer.DisposeAsync();
    }
}
