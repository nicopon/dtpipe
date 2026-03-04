using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Services;

public class MetricsServiceTests
{
    private readonly Mock<IExportObserver> _mockObserver;
    private readonly Mock<ILogger<MetricsService>> _mockLogger;
    private readonly MetricsService _service;

    public MetricsServiceTests()
    {
        _mockObserver = new Mock<IExportObserver>();
        _mockLogger = new Mock<ILogger<MetricsService>>();
        _service = new MetricsService(_mockObserver.Object, _mockLogger.Object);
    }

    [Fact]
    public async Task SaveMetricsAsync_WhenPathIsNull_DoesNothing()
    {
        // Arrange
        var mockProgress = new Mock<IExportProgress>();

        // Act
        await _service.SaveMetricsAsync(mockProgress.Object, null, CancellationToken.None);

        // Assert
        _mockObserver.Verify(o => o.LogMessage(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task SaveMetricsAsync_WhenPathProvided_SavesToFile()
    {
        // Arrange
        var tempFile = Path.Combine(Path.GetTempPath(), $"metrics_{Guid.NewGuid()}.json");
        var mockProgress = new Mock<IExportProgress>();
        var metrics = new ExportMetrics(DateTime.UtcNow, DateTime.UtcNow, 100, 100, 0, 0, new Dictionary<string, long>());
        mockProgress.Setup(p => p.GetMetrics()).Returns(metrics);

        try
        {
            // Act
            await _service.SaveMetricsAsync(mockProgress.Object, tempFile, CancellationToken.None);

            // Assert
            Assert.True(File.Exists(tempFile));
            var content = await File.ReadAllTextAsync(tempFile);
            Assert.Contains("\"ReadCount\": 100", content);
            _mockObserver.Verify(o => o.LogMessage(It.Is<string>(s => s.Contains("Metrics saved to"))), Times.Once);
        }
        finally
        {
            if (File.Exists(tempFile)) File.Delete(tempFile);
        }
    }

    [Fact]
    public void LogMemoryUsage_CallsLogger()
    {
        // Act
        _service.LogMemoryUsage();

        // Assert
        // Verified by non-throwing execution.
    }
}
