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

    // Local test helper implementing ISchemaValidationAware
    private class SchemaSettings : ISchemaValidationAware
    {
        public bool StrictSchema { get; set; }
        public bool NoSchemaValidation { get; set; }
        public bool AutoMigrate { get; set; }
    }

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
        var options = new SchemaSettings { NoSchemaValidation = true };
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
        var options = new SchemaSettings { NoSchemaValidation = false };
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
        var options = new SchemaSettings();
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
        var options = new SchemaSettings { StrictSchema = true };
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
        var options = new SchemaSettings { AutoMigrate = true };
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

    /// <summary>
    /// A writer that inspects its target, so the regime is in force and gets named.
    /// </summary>
    private static Mock<IDataWriter> InspectableWriter(bool exists = true, params TargetColumnInfo[] columns)
    {
        var mockWriter = new Mock<IDataWriter>();
        var mockInspector = mockWriter.As<ISchemaInspector>();
        mockInspector.Setup(i => i.RequiresTargetInspection).Returns(true);
        mockInspector.Setup(i => i.InspectTargetAsync(It.IsAny<CancellationToken>())).ReturnsAsync(
            new TargetSchemaInfo(columns.ToList(), exists, null, null, null));
        return mockWriter;
    }

    [Theory]
    [InlineData(false, false, false, "discard")]
    [InlineData(true, false, false, "freeze (--strict-schema)")]
    [InlineData(false, true, false, "evolve (--auto-migrate)")]
    [InlineData(true, true, false, "evolve+freeze (--auto-migrate --strict-schema)")]
    [InlineData(false, false, true, "off (--no-schema-validation)")]
    public async Task ValidateAndMigrateAsync_NamesTheRegimeInForce(
        bool strict, bool autoMigrate, bool noValidation, string expectedMode)
    {
        // Arrange
        var options = new SchemaSettings { StrictSchema = strict, AutoMigrate = autoMigrate, NoSchemaValidation = noValidation };
        var schema = new List<PipeColumnInfo> { new("ID", typeof(int), false) };
        var mockWriter = InspectableWriter(true, new TargetColumnInfo("ID", "INTEGER", typeof(int), false, true, false));
        mockWriter.As<ISchemaMigrator>();

        var messages = new List<string>();
        _mockObserver.Setup(o => o.LogMessage(It.IsAny<string>())).Callback<string>(messages.Add);

        // Act
        await _service.ValidateAndMigrateAsync(mockWriter.Object, schema, options, CancellationToken.None);

        // Assert
        Assert.Contains(messages, m => m.Contains($"Schema mode: {expectedMode}"));
    }

    /// <summary>
    /// discard and freeze differ only in the guarantee; on a compatible target their outcome lines
    /// are identical, which is why the regime has to be stated separately.
    /// </summary>
    [Fact]
    public async Task ValidateAndMigrateAsync_NamesTheRegime_EvenWhenTheOutcomeIsIdentical()
    {
        var schema = new List<PipeColumnInfo> { new("ID", typeof(int), false) };
        var column = new TargetColumnInfo("ID", "INTEGER", typeof(int), false, true, false);

        var discard = new List<string>();
        _mockObserver.Setup(o => o.LogMessage(It.IsAny<string>())).Callback<string>(discard.Add);
        await _service.ValidateAndMigrateAsync(InspectableWriter(true, column).Object, schema, new SchemaSettings(), CancellationToken.None);

        var freeze = new List<string>();
        _mockObserver.Setup(o => o.LogMessage(It.IsAny<string>())).Callback<string>(freeze.Add);
        await _service.ValidateAndMigrateAsync(
            InspectableWriter(true, column).Object, schema, new SchemaSettings { StrictSchema = true }, CancellationToken.None);

        Assert.Contains(discard, m => m.Contains("Target schema compatible"));
        Assert.Contains(freeze, m => m.Contains("Target schema compatible"));
        Assert.NotEqual(discard, freeze);
    }

    [Fact]
    public async Task ValidateAndMigrateAsync_WhenTargetIsNotInspected_NamesNoRegime()
    {
        // Arrange — a file writer replaces its target, so no regime is in force.
        var options = new SchemaSettings { StrictSchema = true };
        var mockWriter = new Mock<IDataWriter>();
        mockWriter.As<ISchemaInspector>().Setup(i => i.RequiresTargetInspection).Returns(false);

        // Act
        await _service.ValidateAndMigrateAsync(mockWriter.Object, new List<PipeColumnInfo>(), options, CancellationToken.None);

        // Assert
        _mockObserver.Verify(o => o.LogMessage(It.IsAny<string>()), Times.Never);
    }

    [Theory]
    [InlineData(true, false, "'--strict-schema'")]
    [InlineData(false, true, "'--auto-migrate'")]
    [InlineData(true, true, "'--strict-schema' and '--auto-migrate'")]
    public void RejectContradictorySettings_RefusesAFlagThatCancelsAnother(bool strict, bool autoMigrate, string expectedNames)
    {
        var options = new SchemaSettings { NoSchemaValidation = true, StrictSchema = strict, AutoMigrate = autoMigrate };

        var ex = Assert.Throws<InvalidOperationException>(() => SchemaValidationService.RejectContradictorySettings(options));

        Assert.Contains("--no-schema-validation", ex.Message);
        Assert.Contains(expectedNames, ex.Message);
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, true, false)]   // evolve with a net: migrate, re-inspect, then fail if it did not suffice
    [InlineData(false, false, true)]
    public void RejectContradictorySettings_AcceptsEveryRegimeThatMeansSomething(bool strict, bool autoMigrate, bool noValidation)
    {
        var options = new SchemaSettings { StrictSchema = strict, AutoMigrate = autoMigrate, NoSchemaValidation = noValidation };

        SchemaValidationService.RejectContradictorySettings(options);
    }

    [Fact]
    public void RejectContradictorySettings_WithoutSettings_Accepts()
    {
        SchemaValidationService.RejectContradictorySettings(null);
    }

    [Fact]
    public async Task ValidateAndMigrateAsync_WhenFlagsContradict_ThrowsBeforeTouchingTheTarget()
    {
        var options = new SchemaSettings { NoSchemaValidation = true, StrictSchema = true };
        var mockWriter = InspectableWriter();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _service.ValidateAndMigrateAsync(mockWriter.Object, new List<PipeColumnInfo>(), options, CancellationToken.None));

        mockWriter.As<ISchemaInspector>().Verify(i => i.InspectTargetAsync(It.IsAny<CancellationToken>()), Times.Never);
    }
}
