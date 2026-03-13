using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Validation;
using DtPipe.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Services;

public class SchemaValidationServiceTests
{
    private readonly Mock<IExportObserver> _mockObserver;
    private readonly Mock<ILogger<SchemaValidationService>> _mockLogger;
    private readonly SchemaValidationService _service;

    public SchemaValidationServiceTests()
    {
        _mockObserver = new Mock<IExportObserver>();
        _mockLogger = new Mock<ILogger<SchemaValidationService>>();
        _service = new SchemaValidationService(_mockObserver.Object, _mockLogger.Object);
    }

    [Fact]
    public async Task ValidateAndMigrateAsync_WhenNoSchemaValidation_ReturnsImmediately()
    {
        // Arrange
        var options = new PipelineOptions { NoSchemaValidation = true };
        var mockWriter = new Mock<IDataWriter>();
        mockWriter.As<ISchemaInspector>().Setup(i => i.RequiresTargetInspection).Returns(true);
        mockWriter.As<ISchemaInspector>().Setup(i => i.InspectTargetAsync(It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new TargetSchemaInfo(new List<TargetColumnInfo>(), false, null, null, null));
        var schema = new List<PipeColumnInfo>();

        // Act
        await _service.ValidateAndMigrateAsync(mockWriter.Object, schema, options, CancellationToken.None);

        // Assert
        mockWriter.As<ISchemaInspector>().Verify(i => i.InspectTargetAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ValidateAndMigrateAsync_WhenWriterDoesNotImplementInspector_ReturnsImmediately()
    {
        // Arrange
        var options = new PipelineOptions { NoSchemaValidation = false };
        var mockWriter = new Mock<IDataWriter>(); // Only IDataWriter, not ISchemaInspector
        var schema = new List<PipeColumnInfo> { new("col1", typeof(int), true) };

        // Act
        await _service.ValidateAndMigrateAsync(mockWriter.Object, schema, options, CancellationToken.None);

        // Assert
        _mockObserver.Verify(o => o.LogMessage(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task ValidateAndMigrateAsync_WhenCompatible_LogsSuccess()
    {
        // Arrange
        var options = new PipelineOptions();
        var schema = new List<PipeColumnInfo> { new("ID", typeof(int), false) };

        var mockWriter = new Mock<IDataWriter>();
        var mockInspector = mockWriter.As<ISchemaInspector>();
        mockInspector.Setup(i => i.RequiresTargetInspection).Returns(true);
        mockInspector.Setup(i => i.InspectTargetAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new TargetSchemaInfo(
            new List<TargetColumnInfo> { new TargetColumnInfo("ID", "INTEGER", typeof(int), false, true, false) },
            true, null, null, new List<string> { "ID" }));

        // Act
        await _service.ValidateAndMigrateAsync(mockWriter.Object, schema, options, CancellationToken.None);

        // Assert
        _mockObserver.Verify(o => o.LogMessage(It.Is<string>(s => s.Contains("Target schema compatible"))), Times.Once);
    }

    [Fact]
    public async Task ValidateAndMigrateAsync_WhenMissingColAndStrict_ThrowsException()
    {
        // Arrange
        var options = new PipelineOptions { StrictSchema = true };
        var schema = new List<PipeColumnInfo> { new("NEW_COL", typeof(string), true) };

        var mockWriter = new Mock<IDataWriter>();
        var mockInspector = mockWriter.As<ISchemaInspector>();
        mockInspector.Setup(i => i.RequiresTargetInspection).Returns(true);
        mockInspector.Setup(i => i.InspectTargetAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new TargetSchemaInfo(
            new List<TargetColumnInfo>(), true, null, null, null));

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _service.ValidateAndMigrateAsync(mockWriter.Object, schema, options, CancellationToken.None));
    }

    [Fact]
    public async Task ValidateAndMigrateAsync_WhenMissingColAndAutoMigrate_CallsMigrate()
    {
        // Arrange
        var options = new PipelineOptions { AutoMigrate = true };
        var schema = new List<PipeColumnInfo> { new("NEW_COL", typeof(string), true) };

        var mockWriter = new Mock<IDataWriter>();
        var mockInspector = mockWriter.As<ISchemaInspector>();
        mockInspector.Setup(i => i.RequiresTargetInspection).Returns(true);
        mockInspector.Setup(i => i.InspectTargetAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new TargetSchemaInfo(
            new List<TargetColumnInfo>(), true, null, null, null));

        var mockMigrator = mockWriter.As<ISchemaMigrator>();

        // Act
        await _service.ValidateAndMigrateAsync(mockWriter.Object, schema, options, CancellationToken.None);

        // Assert
        mockMigrator.Verify(i => i.MigrateSchemaAsync(It.IsAny<SchemaCompatibilityReport>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}
