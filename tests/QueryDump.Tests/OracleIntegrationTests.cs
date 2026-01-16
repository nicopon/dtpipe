using Testcontainers.Oracle;
using Xunit;
using Oracle.ManagedDataAccess.Client;
using QueryDump.Providers.Oracle;
using QueryDump.Writers;
using QueryDump.Writers.Csv;
using QueryDump.Writers.Parquet;
using QueryDump.Tests.Helpers;

namespace QueryDump.Tests;

/// <summary>
/// Integration tests using Oracle Testcontainers.
/// Requires Docker to be running.
/// </summary>
[Trait("Category", "Integration")]
public class OracleIntegrationTests : IAsyncLifetime
{
    private readonly OracleContainer _oracle = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart")
        .Build();

    public async ValueTask InitializeAsync()
    {
        await _oracle.StartAsync();
        
        await using var connection = new OracleConnection(_oracle.GetConnectionString());
        await connection.OpenAsync();
        
        // Use Seeder for DDL and Data
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "test_data");
        await cmd.ExecuteNonQueryAsync();

        await TestDataSeeder.SeedAsync(connection, "test_data");
    }

    public async ValueTask DisposeAsync()
    {
        await _oracle.DisposeAsync();
    }

    [Fact]
    public async Task OracleStreamReader_ReadsAllRows()
    {
        // Arrange
        var connectionString = _oracle.GetConnectionString();
        
        // Act
        await using var reader = new OracleStreamReader(
            connectionString, 
            "SELECT id, name FROM test_data ORDER BY id",
            new OracleOptions { FetchSize = 65536 });
        
        await reader.OpenAsync(TestContext.Current.CancellationToken);
        
        var rows = new List<object?[]>();
        await foreach (var row in reader.ReadRowsAsync(TestContext.Current.CancellationToken))
        {
            rows.Add(row);
        }
        
        // Assert
        Assert.Equal(4, rows.Count);
        Assert.Equal(2, reader.Columns!.Count);
        Assert.Equal("ID", reader.Columns[0].Name);
    }

    [Fact]
    public async Task ParquetWriter_CreatesValidFile()
    {
        // Arrange
        var connectionString = _oracle.GetConnectionString();
        var outputPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.parquet");
        
        try
        {
            // Act
            await using var reader = new OracleStreamReader(
                connectionString, 
                "SELECT id, name FROM test_data ORDER BY id",
                new OracleOptions { FetchSize = 65536 });
            
            await reader.OpenAsync(TestContext.Current.CancellationToken);
            
            await using var writer = new ParquetDataWriter(outputPath);
            await writer.InitializeAsync(reader.Columns!, TestContext.Current.CancellationToken);
            
            var batch = new List<object?[]>();
            await foreach (var row in reader.ReadRowsAsync(TestContext.Current.CancellationToken))
            {
                batch.Add(row);
            }
            
            await writer.WriteBatchAsync(batch, TestContext.Current.CancellationToken);
            await writer.CompleteAsync(TestContext.Current.CancellationToken);
            
            // Assert
            Assert.True(File.Exists(outputPath));
            Assert.True(new FileInfo(outputPath).Length > 0);
        }
        finally
        {
            if (File.Exists(outputPath))
                File.Delete(outputPath);
        }
    }

    [Fact]
    public async Task CsvWriter_CreatesValidFile()
    {
        // Arrange
        var connectionString = _oracle.GetConnectionString();
        var outputPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.csv");
        
        try
        {
            // Act - write data
            await using (var reader = new OracleStreamReader(
                connectionString, 
                "SELECT id, name FROM test_data ORDER BY id",
                new OracleOptions { FetchSize = 65536 }))
            {
                await reader.OpenAsync(TestContext.Current.CancellationToken);
                
                await using (var writer = new CsvDataWriter(outputPath))
                {
                    await writer.InitializeAsync(reader.Columns!, TestContext.Current.CancellationToken);
                    
                    var batch = new List<object?[]>();
                    await foreach (var row in reader.ReadRowsAsync(TestContext.Current.CancellationToken))
                    {
                        batch.Add(row);
                    }
                    
                    await writer.WriteBatchAsync(batch, TestContext.Current.CancellationToken);
                    await writer.CompleteAsync(TestContext.Current.CancellationToken);
                }
            }
            
            // Assert - after writer is disposed
            Assert.True(File.Exists(outputPath));
            var lines = await File.ReadAllLinesAsync(outputPath, TestContext.Current.CancellationToken);
            Assert.Equal(5, lines.Length); // Header + 4 data rows
        }
        finally
        {
            if (File.Exists(outputPath))
                File.Delete(outputPath);
        }
    }
}
