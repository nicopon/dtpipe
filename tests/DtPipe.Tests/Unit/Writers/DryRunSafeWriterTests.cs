using System;
using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using DtPipe.Adapters.Csv;
using DtPipe.Adapters.Parquet;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Xunit;

namespace DtPipe.Tests.Unit.Writers;

public class DryRunSafeWriterTests : IAsyncLifetime
{
    private string _testParquetPath = null!;
    private string _testCsvPath = null!;

    public ValueTask InitializeAsync()
    {
        _testParquetPath = Path.Combine(Path.GetTempPath(), $"dryrun_safe_{Guid.NewGuid()}.parquet");
        _testCsvPath = Path.Combine(Path.GetTempPath(), $"dryrun_safe_{Guid.NewGuid()}.csv");
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (File.Exists(_testParquetPath)) File.Delete(_testParquetPath);
        if (File.Exists(_testCsvPath)) File.Delete(_testCsvPath);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void ParquetWriter_Constructor_ShouldNotCreateFile()
    {
        // Act
        var writer = new ParquetDataWriter(_testParquetPath);

        // Assert
        File.Exists(_testParquetPath).Should().BeFalse("instantiation should not create file");
    }

    [Fact]
    public async Task ParquetWriter_InitializeAsync_ShouldCreateFile()
    {
        // Arrange
        var writer = new ParquetDataWriter(_testParquetPath);
        var columns = new List<ColumnInfo> { new("Id", typeof(int), false) };

        // Act
        await writer.InitializeAsync(columns);

        // Assert
        File.Exists(_testParquetPath).Should().BeTrue("InitializeAsync should create file");
        await writer.DisposeAsync();
    }

    [Fact]
    public void CsvWriter_Constructor_ShouldNotCreateFile()
    {
        // Act
        var writer = new CsvDataWriter(_testCsvPath);

        // Assert
        File.Exists(_testCsvPath).Should().BeFalse("instantiation should not create file");
    }
    
    [Fact]
    public async Task CsvWriter_InitializeAsync_ShouldCreateFile()
    {
        // Arrange
        var writer = new CsvDataWriter(_testCsvPath);
        var columns = new List<ColumnInfo> { new("Id", typeof(int), false) };

        // Act
        await writer.InitializeAsync(columns);

        // Assert
        File.Exists(_testCsvPath).Should().BeTrue("InitializeAsync should create file");
        await writer.DisposeAsync();
    }
}
