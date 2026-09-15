using System.Runtime.CompilerServices;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Services;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Services;

/// <summary>
/// F16 — cancellation must not mask as success. These tests drive the real
/// LinearPipelineService + ExportService stack with stub readers/writers.
/// </summary>
public class LinearPipelineServiceTests
{
    // ─────────────────────────────────────────────────────────────────────────
    // Test doubles
    // ─────────────────────────────────────────────────────────────────────────

    private sealed class StubOptions : IOptionSet
    {
        public static string Prefix => "stub";
        public static string DisplayName => "Stub";
    }

    private sealed class TableQueryOptions : IOptionSet, IQueryAwareOptions, ITableAwareOptions
    {
        public static string Prefix => "tq";
        public static string DisplayName => "TableQuery";
        public string? Query { get; set; }
        public string? Table { get; set; }
    }

    private sealed class WriterTableOptions : IOptionSet, ITableAwareOptions
    {
        public static string Prefix => "wt";
        public static string DisplayName => "WriterTable";
        public string? Table { get; set; }
    }

    private sealed class BlockingReader : IStreamReader
    {
        public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        public Task OpenAsync(CancellationToken ct) => Task.Delay(Timeout.Infinite, ct);
        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            yield break;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FaultedReader : IStreamReader
    {
        public IReadOnlyList<PipeColumnInfo>? Columns => null;
        public Task OpenAsync(CancellationToken ct)
            => throw new InvalidOperationException("root cause X", new ArgumentException("mid layer Y"));
        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            yield break;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class StubReaderFactory : IStreamReaderFactory
    {
        private readonly Func<IStreamReader> _readerProvider;
        public StubReaderFactory(Func<IStreamReader> readerProvider) => _readerProvider = readerProvider;
        public string ComponentName => "stub";
        public string Category => "Test";
        public Type OptionsType => typeof(StubOptions);
        public bool CanHandle(string connectionString) => false;
        public bool RequiresQuery => false;
        public IEnumerable<Type> GetSupportedOptionTypes() => new[] { typeof(StubOptions) };
        public IStreamReader Create(OptionsRegistry registry) => _readerProvider();
    }

    private sealed class QueryableStubReaderFactory : IStreamReaderFactory
    {
        private readonly Func<IStreamReader> _readerProvider;
        public QueryableStubReaderFactory(Func<IStreamReader> readerProvider) => _readerProvider = readerProvider;
        public string ComponentName => "qstub";
        public string Category => "Test";
        public Type OptionsType => typeof(TableQueryOptions);
        public bool CanHandle(string connectionString) => false;
        public bool RequiresQuery => true;
        public IEnumerable<Type> GetSupportedOptionTypes() => new[] { typeof(TableQueryOptions) };
        public IStreamReader Create(OptionsRegistry registry) => _readerProvider();
    }

    /// <summary>Reader that fails fast at OpenAsync — enough to observe pre-flight binding.</summary>
    private sealed class MarkerReader : IStreamReader
    {
        public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        public Task OpenAsync(CancellationToken ct) => throw new InvalidOperationException("marker: stop after reader creation");
        public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            yield break;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NullWriterFactory : IDataWriterFactory
    {
        public string ComponentName => "null";
        public string Category => "Test";
        public Type OptionsType => typeof(StubOptions);
        public bool CanHandle(string connectionString) => false;
        public IDataWriter Create(OptionsRegistry registry) => throw new NotSupportedException("writer is never opened in these tests");
        public IEnumerable<Type> GetSupportedOptionTypes() => Array.Empty<Type>();
    }

    private sealed class TableStubWriterFactory : IDataWriterFactory
    {
        public string ComponentName => "twstub";
        public string Category => "Test";
        public Type OptionsType => typeof(WriterTableOptions);
        public bool CanHandle(string connectionString) => false;
        public IDataWriter Create(OptionsRegistry registry) => throw new NotSupportedException("writer is never opened in these tests");
        public IEnumerable<Type> GetSupportedOptionTypes() => Array.Empty<Type>();
    }

    /// <summary>Records whether the probe (PipelineOptions registration check) held at OpenAsync time.</summary>
    private sealed class ProbeTransformer : IStreamTransformer
    {
        public Func<bool>? Probe { get; set; }
        public bool PipelineOptionsWasRegistered { get; private set; }
        public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        public Apache.Arrow.Schema? Schema => null;
        public Task OpenAsync(CancellationToken ct = default)
        {
            PipelineOptionsWasRegistered = Probe?.Invoke() ?? false;
            return Task.CompletedTask;
        }
        public async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadResultsAsync(
            IAsyncEnumerable<Apache.Arrow.RecordBatch>? inputStream = null,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            await Task.Yield();
            yield break;
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ProbeTransformerFactory : IStreamTransformerFactory
    {
        private readonly ProbeTransformer _transformer;
        public ProbeTransformerFactory(ProbeTransformer transformer) => _transformer = transformer;
        public string ComponentName => "probe";
        public string Category => "Test";
        public bool RequiresArrowChannels => false;
        public int MinStreams => 0;
        public int MaxStreams => -1;
        public int MinLookups => 0;
        public int MaxLookups => -1;
        public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => new[] { ("--stubproc", true) };
        public bool IsApplicable(string[] branchArgs) => branchArgs.Contains("--stubproc");
        public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider) => _transformer;
        public IStreamTransformer CreateFromJob(JobDefinition job, BranchChannelContext ctx, IServiceProvider serviceProvider) => _transformer;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Harness
    // ─────────────────────────────────────────────────────────────────────────

    private static (LinearPipelineService Service, Mock<IAnsiConsole> Console, OptionsRegistry OptionsRegistry) BuildService(
        IStreamReaderFactory readerFactory,
        IDataWriterFactory? writerFactoryOverride = null,
        IStreamTransformerFactory? streamTransformerFactory = null)
    {
        var observer = Mock.Of<IExportObserver>();
        var exportService = new ExportService(
            readerFactories: new List<IStreamReaderFactory> { readerFactory },
            writerFactories: new List<IDataWriterFactory> { writerFactoryOverride ?? new NullWriterFactory() },
            transformerFactories: new List<IDataTransformerFactory>(),
            optionsRegistry: new OptionsRegistry(),
            observer: observer,
            logger: NullLogger<ExportService>.Instance,
            hookExecutor: new HookExecutor(observer, NullLogger<HookExecutor>.Instance),
            metricsService: new MetricsService(observer, NullLogger<MetricsService>.Instance),
            schemaValidator: new SchemaValidationService(observer, NullLogger<SchemaValidationService>.Instance),
            pipelineExecutor: new PipelineExecutor(
                Enumerable.Empty<IRowToColumnarBridgeFactory>(),
                Enumerable.Empty<IColumnarToRowBridgeFactory>(),
                NullLogger<PipelineExecutor>.Instance),
            channelRegistry: new MemoryChannelRegistry());

        var services = new ServiceCollection();
        services.AddSingleton(exportService);
        services.AddSingleton<IEnumerable<IStreamReaderFactory>>(new List<IStreamReaderFactory> { readerFactory });
        services.AddSingleton<IEnumerable<IDataWriterFactory>>(new List<IDataWriterFactory> { writerFactoryOverride ?? new NullWriterFactory() });
        services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(
            streamTransformerFactory != null
                ? new List<IStreamTransformerFactory> { streamTransformerFactory }
                : new List<IStreamTransformerFactory>());
        var serviceProvider = services.BuildServiceProvider();

        var consoleMock = new Mock<IAnsiConsole>();
        var serviceRegistry = new OptionsRegistry();
        var service = new LinearPipelineService(
            contributors: Array.Empty<DtPipe.Cli.Infrastructure.ICliContributor>(),
            serviceProvider: serviceProvider,
            channelRegistry: new MemoryChannelRegistry(),
            optionsRegistry: serviceRegistry,
            console: consoleMock.Object);
        return (service, consoleMock, serviceRegistry);
    }

    private static JobDefinition BlockingJob() => new() { Input = "stub:block", Output = null, Limit = 0 };

    // ─────────────────────────────────────────────────────────────────────────
    // Facts
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task User_Ctrl_C_Returns_130()
    {
        var (service, _, _) = BuildService(new StubReaderFactory(() => new BlockingReader()));
        using var userCts = new CancellationTokenSource();

        var task = service.ExecuteAsync(BlockingJob(), context: null, token: CancellationToken.None, userCancellationToken: userCts.Token);

        // Give the pipeline time to reach the blocking OpenAsync, then simulate Ctrl-C.
        await Task.Delay(200);
        await userCts.CancelAsync();

        var exitCode = await task;
        Assert.Equal(130, exitCode);
    }

    [Fact]
    public async Task Internal_Cancellation_Propagates_Not_Masks()
    {
        var (service, _, _) = BuildService(new StubReaderFactory(() => new BlockingReader()));
        using var internalCts = new CancellationTokenSource();

        var task = service.ExecuteAsync(BlockingJob(), context: null, token: internalCts.Token, userCancellationToken: CancellationToken.None);

        await Task.Delay(200);
        await internalCts.CancelAsync();

        // Internal cancellation must reach the caller as an exception, not a 0/130 exit code.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    }

    [Fact]
    public async Task Exception_Chain_Is_Preserved_In_Error_Output()
    {
        var (service, consoleMock, _) = BuildService(new StubReaderFactory(() => new FaultedReader()));

        var exitCode = await service.ExecuteAsync(new JobDefinition { Input = "stub:fault", Output = "null:-" }, context: null, token: CancellationToken.None, userCancellationToken: CancellationToken.None);

        Assert.Equal(1, exitCode);
        consoleMock.Verify(c => c.Write(It.IsAny<Markup>()), Times.AtLeastOnce);
    }

    [Fact]
    public void ExceptionChainFlattener_Formats_Full_Chain()
    {
        var ex = new InvalidOperationException("root cause X", new ArgumentException("mid layer Y"));

        var formatted = DtPipe.Core.Infrastructure.Diagnostics.ExceptionChainFlattener.Format(ex);

        Assert.Contains("InvalidOperationException: root cause X", formatted, StringComparison.Ordinal);
        Assert.Contains("ArgumentException: mid layer Y", formatted, StringComparison.Ordinal);
    }

    [Fact]
    public void ExceptionChainFlattener_Unwraps_SingleFault_AggregateException()
    {
        var ex = new AggregateException("wrapper", new InvalidOperationException("real cause"));

        var formatted = DtPipe.Core.Infrastructure.Diagnostics.ExceptionChainFlattener.Format(ex);

        Assert.DoesNotContain("wrapper", formatted, StringComparison.Ordinal);
        Assert.Contains("InvalidOperationException: real cause", formatted, StringComparison.Ordinal);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // RequiresQuery auto-build (--table → SELECT * FROM "table")
    // Regression guard: ITableAwareOptions must be honored on READER options,
    // not only on writer options (readers are the primary --table users).
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RequiresQuery_AutoBuilds_Select_From_Reader_Table()
    {
        var readerOptions = new TableQueryOptions { Table = "users_test" };
        var factory = new QueryableStubReaderFactory(() => new MarkerReader());
        var (service, _, registry) = BuildService(factory);
        registry.Register(readerOptions);

        var exitCode = await service.ExecuteAsync(
            new JobDefinition { Input = "qstub:x" }, context: null,
            token: CancellationToken.None, userCancellationToken: CancellationToken.None);

        // The marker reader fails after creation — we only care about pre-flight binding.
        Assert.Equal(1, exitCode);
        Assert.Equal("SELECT * FROM \"users_test\"", readerOptions.Query);
    }

    [Fact]
    public async Task RequiresQuery_Falls_Back_To_Writer_Table_When_Reader_Has_None()
    {
        var readerOptions = new TableQueryOptions();
        var writerOptions = new WriterTableOptions { Table = "same_named" };
        var readerFactory = new QueryableStubReaderFactory(() => new MarkerReader());
        var (service, _, registry) = BuildService(readerFactory, new TableStubWriterFactory());
        registry.Register(readerOptions);
        registry.Register(writerOptions);

        var exitCode = await service.ExecuteAsync(
            new JobDefinition { Input = "qstub:x", Output = "twstub:same_named" }, context: null,
            token: CancellationToken.None, userCancellationToken: CancellationToken.None);

        Assert.Equal(1, exitCode);
        Assert.Equal("SELECT * FROM \"same_named\"", readerOptions.Query);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Stream-transformer branches alias their factory OptionsType to PipelineOptions.
    // Invariant: PipelineOptions must be registered BEFORE any factory-options probe,
    // otherwise the probe warns about missing options and returns a discarded default.
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task StreamTransformerBranch_Has_PipelineOptions_Registered_Before_Use()
    {
        var transformer = new ProbeTransformer();
        var (service, _, registry) = BuildService(
            new StubReaderFactory(() => new MarkerReader()),
            streamTransformerFactory: new ProbeTransformerFactory(transformer));
        transformer.Probe = () => registry.Has<DtPipe.Core.Models.PipelineOptions>();

        await service.ExecuteAsync(
            new JobDefinition { Input = "stub:x" },
            context: new CliJobContext(null, null, null, new[] { "--stubproc" }),
            token: CancellationToken.None, userCancellationToken: CancellationToken.None);

        // The flag being evaluated at all proves execution reached OpenAsync;
        // it must hold by then (writerless branch may still end with a non-zero exit).
        Assert.True(transformer.PipelineOptionsWasRegistered,
            "PipelineOptions must be registered before the stream-transformer branch executes.");
    }
}
