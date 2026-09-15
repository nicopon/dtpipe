using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Services;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Services;

public class ExportServiceTests
{
	private readonly Mock<IStreamReaderFactory> _mockReaderFactory;
	private readonly Mock<IDataWriterFactory> _mockWriterFactory;
	private readonly Mock<IExportObserver> _mockObserver;
	private readonly Mock<IExportProgress> _mockProgress;
	private readonly Mock<ILogger<ExportService>> _mockLogger;
	private readonly ExportService _service;

	public ExportServiceTests()
	{
		_mockReaderFactory = new Mock<IStreamReaderFactory>();
		_mockWriterFactory = new Mock<IDataWriterFactory>();
		_mockObserver = new Mock<IExportObserver>();
		_mockProgress = new Mock<IExportProgress>();
		_mockLogger = new Mock<ILogger<ExportService>>();

		var readerFactoryList = new List<IStreamReaderFactory> { _mockReaderFactory.Object };
		var writerFactoryList = new List<IDataWriterFactory> { _mockWriterFactory.Object };

		_mockObserver.Setup(x => x.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IReadOnlyList<(string Name, bool IsColumnar)>>(), It.IsAny<bool>(), It.IsAny<string?>(), It.IsAny<bool>()))
					 .Returns(_mockProgress.Object);

		_mockProgress.Setup(p => p.GetMetrics())
					 .Returns(new ExportMetrics(DateTime.UtcNow, DateTime.UtcNow, 1, 1, 0, 0, new Dictionary<string, long>()));

		var hookExecutor = new HookExecutor(_mockObserver.Object, new Mock<ILogger<HookExecutor>>().Object);
		var metricsService = new MetricsService(_mockObserver.Object, new Mock<ILogger<MetricsService>>().Object);
		var schemaValidator = new SchemaValidationService(_mockObserver.Object, new Mock<ILogger<SchemaValidationService>>().Object);
		var pipelineExecutor = new PipelineExecutor(
			new List<IRowToColumnarBridgeFactory>(),
			new List<IColumnarToRowBridgeFactory>(),
			new Mock<ILogger<PipelineExecutor>>().Object);

		_service = new ExportService(
			readerFactoryList,
			writerFactoryList,
			new List<IDataTransformerFactory>(),
			new OptionsRegistry(),
			_mockObserver.Object,
			_mockLogger.Object,
			hookExecutor,
			metricsService,
			schemaValidator,
			pipelineExecutor
		);
	}

	[Fact]
	public async Task RunExportAsync_CallsObserverMethods()
	{
		// Arrange
		var options = new PipelineOptions
		{
			NoStats = true
		};

		var cts = new CancellationTokenSource();
		var pipeline = new List<IDataTransformer>(); // Empty pipeline

		// Mock Reader
		var mockReader = new Mock<IStreamReader>();
		mockReader.Setup(r => r.OpenAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
		mockReader.Setup(r => r.Columns).Returns(new List<PipeColumnInfo> { new("col1", typeof(int), true) });
		mockReader.Setup(r => r.ReadBatchesAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
				  .Returns(ToAsyncEnumerable(new[] { new object?[] { 1 } }));
		mockReader.Setup(r => r.DisposeAsync()).Returns(ValueTask.CompletedTask);

		_mockReaderFactory.Setup(f => f.OptionsType).Returns(typeof(EmptyOptions));
		_mockReaderFactory.Setup(f => f.Create(It.IsAny<OptionsRegistry>())).Returns(mockReader.Object);

		// Mock Writer
		var mockWriter = new Mock<IRowDataWriter>();
		mockWriter.Setup(w => w.InitializeAsync(It.IsAny<IReadOnlyList<PipeColumnInfo>>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		// Correctly match IReadOnlyList<object?[]>
		mockWriter.Setup(w => w.WriteBatchAsync(It.IsAny<IReadOnlyList<object?[]>>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		mockWriter.Setup(w => w.CompleteAsync(It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		mockWriter.Setup(w => w.DisposeAsync()).Returns(ValueTask.CompletedTask);

		_mockWriterFactory.Setup(f => f.ComponentName).Returns("test-target");
		_mockWriterFactory.Setup(f => f.OptionsType).Returns(typeof(EmptyOptions));
		_mockWriterFactory.Setup(f => f.Create(It.IsAny<OptionsRegistry>())).Returns(mockWriter.Object);

		// Act
		await _service.RunExportAsync(new PipelineOptions { NoStats = options.NoStats }, "test-source", "target-path", cts.Token, pipeline, _mockReaderFactory.Object, _mockWriterFactory.Object, new OptionsRegistry(), showStatusMessages: true);

		// Assert
		_mockObserver.Verify(o => o.ShowIntro("test-source", "target-path"), Times.Once);
		_mockObserver.Verify(o => o.ShowConnectionStatus(false, null), Times.Once); // Connecting...
		_mockObserver.Verify(o => o.ShowConnectionStatus(true, 1), Times.Once); // Connected
		_mockObserver.Verify(o => o.ShowTarget("test-target", "target-path"), Times.Once);
		_mockObserver.Verify(o => o.CreateProgressReporter(false, It.IsAny<IReadOnlyList<(string Name, bool IsColumnar)>>(), It.IsAny<bool>(), It.IsAny<string?>(), It.IsAny<bool>()), Times.Once);

		_mockProgress.Verify(p => p.ReportRead(1), Times.AtLeastOnce);
		_mockProgress.Verify(p => p.ReportWrite(1), Times.AtLeastOnce);
		_mockProgress.Verify(p => p.Complete(), Times.Once);
	}

	private static async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ToAsyncEnumerable(IEnumerable<object?[]> data)
	{
		yield return new ReadOnlyMemory<object?[]>(System.Linq.Enumerable.ToArray(data));
		await Task.CompletedTask;
	}
}

public class PipelinePhaseOrderTests
{
    /// <summary>
    /// P1-8 — phase composition: a successful run persists metrics (MetricsPhase) only
    /// after the writer completed its output (ExecutionPhase), and hook files run in the
    /// documented order. Uses real temp files as observable phase markers.
    /// </summary>
    [Fact]
    public async Task Phases_Execute_In_Documented_Order()
    {
        var dir = Path.Combine(Path.GetTempPath(), "dtpipe-phase-order-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var outputCsv = Path.Combine(dir, "out.csv");
        var metrics = Path.Combine(dir, "metrics.json");
        var preMarker = Path.Combine(dir, "pre.marker");
        var postMarker = Path.Combine(dir, "post.marker");
        var finallyMarker = Path.Combine(dir, "finally.marker");

        try
        {
            var registry = new DtPipe.Core.Options.OptionsRegistry();
            var readerFactory = new PhaseStubReaderFactory();
            var writerFactory = new PhaseStubWriterFactory(outputCsv);

            registry.Register(new DtPipe.Adapters.Csv.CsvWriterOptions());
            var writerOpts = registry.Get<DtPipe.Adapters.Csv.CsvWriterOptions>();
            typeof(DtPipe.Adapters.Csv.CsvWriterOptions).GetProperty("PreExec")?.SetValue(writerOpts, $"touch {preMarker}");
            typeof(DtPipe.Adapters.Csv.CsvWriterOptions).GetProperty("PostExec")?.SetValue(writerOpts, $"touch {postMarker}");
            typeof(DtPipe.Adapters.Csv.CsvWriterOptions).GetProperty("FinallyExec")?.SetValue(writerOpts, $"touch {finallyMarker}");

            var svc = BuildExportService(registry, readerFactory, writerFactory);
            await svc.RunExportAsync(
                new PipelineOptions { MetricsPath = metrics },
                providerName: "generate",
                outputPath: outputCsv,
                CancellationToken.None,
                pipeline: new List<IDataTransformer>(),
                readerFactory,
                writerFactory,
                registry);

            File.Exists(outputCsv).Should().BeTrue("ExecutionPhase wrote the output");
            File.Exists(metrics).Should().BeTrue("MetricsPhase persisted metrics after execution");

            var preT = File.GetLastWriteTimeUtc(preMarker);
            var postT = File.GetLastWriteTimeUtc(postMarker);
            var finT = File.GetLastWriteTimeUtc(finallyMarker);
            preT.Should().BeOnOrBefore(postT, "Pre-Hook runs before Post-Hook");
            postT.Should().BeOnOrBefore(finT, "Post-Hook runs before Finally-Hook");
        }
        finally
        {
            TryDelete(preMarker); TryDelete(postMarker); TryDelete(finallyMarker); TryDeleteDirectory(dir);
        }
    }

    internal static ExportService BuildExportService(
        DtPipe.Core.Options.OptionsRegistry registry,
        IStreamReaderFactory readerFactory,
        IDataWriterFactory writerFactory)
    {
        var observer = new Moq.Mock<IExportObserver>();
        observer.Setup(o => o.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<System.Collections.Generic.IReadOnlyList<(string Name, bool IsColumnar)>>(),
                        It.IsAny<bool>(), It.IsAny<string?>(), It.IsAny<bool>()))
                .Returns(new StubExportProgress());
        return new ExportService(
            new List<IStreamReaderFactory> { readerFactory },
            new List<IDataWriterFactory> { writerFactory },
            new List<IDataTransformerFactory>(),
            registry,
            observer.Object,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ExportService>.Instance,
            new HookExecutor(observer.Object, Microsoft.Extensions.Logging.Abstractions.NullLogger<HookExecutor>.Instance),
            new MetricsService(observer.Object, Microsoft.Extensions.Logging.Abstractions.NullLogger<MetricsService>.Instance),
            new SchemaValidationService(observer.Object, Microsoft.Extensions.Logging.Abstractions.NullLogger<SchemaValidationService>.Instance),
            new PipelineExecutor(
                Enumerable.Empty<IRowToColumnarBridgeFactory>(),
                Enumerable.Empty<IColumnarToRowBridgeFactory>(),
                Microsoft.Extensions.Logging.Abstractions.NullLogger<PipelineExecutor>.Instance));
    }

    private static void TryDelete(string f) { try { if (File.Exists(f)) File.Delete(f); } catch { } }
    private static void TryDeleteDirectory(string d) { try { if (Directory.Exists(d)) Directory.Delete(d, true); } catch { }
    }
}

internal sealed class PhaseStubReaderFactory : IStreamReaderFactory
{
    public string ComponentName => "generate";
    public string Category => "Test";
    public Type OptionsType => typeof(PhaseStubOptions);
    public bool CanHandle(string cs) => false;
    public bool RequiresQuery => false;
    public IEnumerable<Type> GetSupportedOptionTypes() => new[] { OptionsType };
    public IStreamReader Create(OptionsRegistry registry) => new TwoRowReader();
}

internal sealed class PhaseStubOptions : IOptionSet
{
    public static string Prefix => "gen";
    public static string DisplayName => "Gen";
}

internal sealed class TwoRowReader : IStreamReader
{
    public IReadOnlyList<PipeColumnInfo>? Columns => new List<PipeColumnInfo> { new("Id", typeof(int), false) };
    public Task OpenAsync(CancellationToken ct) => Task.CompletedTask;
    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        yield return new ReadOnlyMemory<object?[]>(new[] { new object?[] { 1 }, new object?[] { 2 } });
        await Task.Yield();
    }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class PhaseStubWriterFactory(string outputPath) : IDataWriterFactory
{
    public string ComponentName => "csv";
    public string Category => "Test";
    public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvWriterOptions);
    public bool CanHandle(string cs) => false;
    public IEnumerable<Type> GetSupportedOptionTypes() => new[] { OptionsType };
    public IDataWriter Create(OptionsRegistry registry) => new MarkerWriter(outputPath);
}

/// <summary>Writes the output file at Initialize — the observable ExecutionPhase marker.</summary>
internal sealed class MarkerWriter(string outputPath) : IRowDataWriter
{
    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        File.WriteAllText(outputPath, "Id\n1\n2\n");
        return ValueTask.CompletedTask;
    }
    public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}


internal sealed class StubExportProgress : IExportProgress
{
    public void ReportRead(int count) { }
    public void ReportTransform(string transformerName, int count) { }
    public void ReportWrite(int count) { }
    public void Complete() { }
    public ExportMetrics GetMetrics() => new ExportMetrics(DateTime.UtcNow, DateTime.UtcNow, 0, 0, 0, 0,
        new System.Collections.Generic.Dictionary<string, long>());
    public void Dispose() { }
}
