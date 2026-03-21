using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Services;
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

		_mockObserver.Setup(x => x.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IEnumerable<string>>()))
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
			Provider = "test-source",
			ConnectionString = "source-conn",
			Query = "SELECT * FROM table",
			OutputPath = "target-path",
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

		_mockReaderFactory.Setup(f => f.Create(It.IsAny<OptionsRegistry>())).Returns(mockReader.Object);

		// Mock Writer
		var mockWriter = new Mock<IDataWriter>();
		mockWriter.Setup(w => w.InitializeAsync(It.IsAny<IReadOnlyList<PipeColumnInfo>>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		// Correctly match IReadOnlyList<object?[]>
		mockWriter.Setup(w => w.WriteBatchAsync(It.IsAny<IReadOnlyList<object?[]>>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		mockWriter.Setup(w => w.CompleteAsync(It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		mockWriter.Setup(w => w.DisposeAsync()).Returns(ValueTask.CompletedTask);

		_mockWriterFactory.Setup(f => f.ComponentName).Returns("test-target");
		_mockWriterFactory.Setup(f => f.Create(It.IsAny<OptionsRegistry>())).Returns(mockWriter.Object);

		// Act
		await _service.RunExportAsync(new PipelineOptions { NoStats = options.NoStats }, options.Provider, options.OutputPath, cts.Token, pipeline, _mockReaderFactory.Object, _mockWriterFactory.Object, new OptionsRegistry());

		// Assert
		_mockObserver.Verify(o => o.ShowIntro("test-source", "target-path"), Times.Once);
		_mockObserver.Verify(o => o.ShowConnectionStatus(false, null), Times.Once); // Connecting...
		_mockObserver.Verify(o => o.ShowConnectionStatus(true, 1), Times.Once); // Connected
		_mockObserver.Verify(o => o.ShowTarget("test-target", "target-path"), Times.Once);
		_mockObserver.Verify(o => o.CreateProgressReporter(false, It.IsAny<IEnumerable<string>>()), Times.Once);

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
