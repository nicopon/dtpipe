using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
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

		_service = new ExportService(
			readerFactoryList,
			writerFactoryList,
			new List<IDataTransformerFactory>(), // transformers
			new OptionsRegistry(), // options registry
			_mockObserver.Object,
			_mockLogger.Object
		);
	}

	[Fact]
	public async Task RunExportAsync_CallsObserverMethods()
	{
		// Arrange
		var options = new DumpOptions
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

		_mockReaderFactory.Setup(f => f.Create(It.IsAny<DumpOptions>())).Returns(mockReader.Object);

		// Mock Writer
		var mockWriter = new Mock<IDataWriter>();
		mockWriter.Setup(w => w.InitializeAsync(It.IsAny<IReadOnlyList<PipeColumnInfo>>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		// Correctly match IReadOnlyList<object?[]>
		mockWriter.Setup(w => w.WriteBatchAsync(It.IsAny<IReadOnlyList<object?[]>>(), It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		mockWriter.Setup(w => w.CompleteAsync(It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
		mockWriter.Setup(w => w.DisposeAsync()).Returns(ValueTask.CompletedTask);

		_mockWriterFactory.Setup(f => f.ProviderName).Returns("test-target");
		_mockWriterFactory.Setup(f => f.Create(It.IsAny<DumpOptions>())).Returns(mockWriter.Object);

		// Act
		await _service.RunExportAsync(options, cts.Token, pipeline, _mockReaderFactory.Object, _mockWriterFactory.Object);

		// Assert
		_mockObserver.Verify(o => o.ShowIntro("test-source", "source-conn"), Times.Once);
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
