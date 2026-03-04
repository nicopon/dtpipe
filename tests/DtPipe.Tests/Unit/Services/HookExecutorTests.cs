using DtPipe.Core.Abstractions;
using DtPipe.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Services;

public class HookExecutorTests
{
    private readonly Mock<IExportObserver> _mockObserver;
    private readonly Mock<ILogger<HookExecutor>> _mockLogger;
    private readonly HookExecutor _executor;

    public HookExecutorTests()
    {
        _mockObserver = new Mock<IExportObserver>();
        _mockLogger = new Mock<ILogger<HookExecutor>>();
        _executor = new HookExecutor(_mockObserver.Object, _mockLogger.Object);
    }

    [Fact]
    public async Task ExecuteAsync_WhenCommandIsNull_DoesNothing()
    {
        // Arrange
        var mockWriter = new Mock<IDataWriter>();

        // Act
        await _executor.ExecuteAsync(mockWriter.Object, "TestHook", null, CancellationToken.None);

        // Assert
        mockWriter.Verify(w => w.ExecuteCommandAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _mockObserver.Verify(o => o.LogMessage(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_WhenCommandIsProvided_CallsWriterAndObserver()
    {
        // Arrange
        var mockWriter = new Mock<IDataWriter>();
        mockWriter.Setup(w => w.ExecuteCommandAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                  .Returns(ValueTask.CompletedTask);

        // Act
        await _executor.ExecuteAsync(mockWriter.Object, "Pre-Export", "CREATE TABLE temp", CancellationToken.None);

        // Assert
        _mockObserver.Verify(o => o.OnHookExecuting("Pre-Export", "CREATE TABLE temp"), Times.Once);
        mockWriter.Verify(w => w.ExecuteCommandAsync("CREATE TABLE temp", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_WriterThrows_PropagatesException()
    {
        // Arrange
        var mockWriter = new Mock<IDataWriter>();
        mockWriter.Setup(w => w.ExecuteCommandAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                  .ThrowsAsync(new System.Exception("DB Error"));

        // Act & Assert
        await Assert.ThrowsAsync<System.Exception>(() =>
            _executor.ExecuteAsync(mockWriter.Object, "ErrorHook", "FAIL", CancellationToken.None));
    }
}
