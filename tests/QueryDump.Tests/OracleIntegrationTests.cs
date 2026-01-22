using Testcontainers.Oracle;
using Xunit;
using Oracle.ManagedDataAccess.Client;
using QueryDump.Adapters.Oracle;
using QueryDump.Adapters.DuckDB;
using QueryDump.Core;
using QueryDump.Adapters;
using QueryDump.Adapters.Csv;
using QueryDump.Adapters.Parquet;
using QueryDump.Tests.Helpers;

namespace QueryDump.Tests;

/// <summary>
/// Integration tests using Oracle Testcontainers.
/// Requires Docker to be running.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class OracleIntegrationTests : IAsyncLifetime
{
    private OracleContainer? _oracle;

    public async ValueTask InitializeAsync()
    {
        if (!DockerHelper.IsAvailable())
        {
            return;
        }

        try 
        {
            _oracle = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart").Build();
            await _oracle.StartAsync();
            
            await using var connection = new OracleConnection(_oracle.GetConnectionString());
            await connection.OpenAsync();
            
            // Use Seeder for DDL and Data
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "test_data");
            await cmd.ExecuteNonQueryAsync();

            await TestDataSeeder.SeedAsync(connection, "test_data");
        }
        catch (Exception)
        {
            // If Docker fails to start despite checks, we treat it as unavailable
            _oracle = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_oracle is not null)
        {
            await _oracle.DisposeAsync();
        }
    }

    [Fact]
    public async Task OracleStreamReader_ReadsAllRows()
    {
        if (!DockerHelper.IsAvailable() || _oracle is null) return;

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
        if (!DockerHelper.IsAvailable() || _oracle is null) return;

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
        if (!DockerHelper.IsAvailable() || _oracle is null) return;

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

    [Fact]
    public async Task OracleDataWriter_BulkCopy_FromDuckDb_ToOracle()
    {
        if (!DockerHelper.IsAvailable() || _oracle is null) return;

        // Arrange: Create in-memory DuckDB source with test data
        var duckDbPath = Path.Combine(Path.GetTempPath(), $"test_source_{Guid.NewGuid()}.duckdb");
        var duckConnectionString = $"Data Source={duckDbPath}";
        
        try
        {
            // Seed DuckDB source
            await using (var duckConnection = new DuckDB.NET.Data.DuckDBConnection(duckConnectionString))
            {
                await duckConnection.OpenAsync(TestContext.Current.CancellationToken);
                await using var cmd = duckConnection.CreateCommand();
                cmd.CommandText = TestDataSeeder.GenerateTableDDL(duckConnection, "source_data");
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
                await TestDataSeeder.SeedAsync(duckConnection, "source_data");
            }

            // Act: Read from DuckDB, write to Oracle
            var targetTable = $"TARGET_{Guid.NewGuid():N}".Substring(0, 20).ToUpperInvariant();
            
            await using var reader = new DuckDataSourceReader(
                duckConnectionString,
                "SELECT Id, Name, Score FROM source_data ORDER BY Id",
                new DuckDbOptions());
            
            await reader.OpenAsync(TestContext.Current.CancellationToken);
            var columns = reader.Columns!;
            
            var writerOptions = new OracleWriterOptions
            {
                Table = targetTable,
                Strategy = OracleWriteStrategy.Truncate,
                BulkSize = 1000 // Enable bulk copy with IDataReader
            };
            
            await using var writer = new OracleDataWriter(_oracle.GetConnectionString(), writerOptions);
            await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
            
            // Read all rows and write in batches
            await foreach (var batch in reader.ReadBatchesAsync(100, TestContext.Current.CancellationToken))
            {
                await writer.WriteBatchAsync(batch.ToArray(), TestContext.Current.CancellationToken);
            }
            
            await writer.CompleteAsync(TestContext.Current.CancellationToken);

            // Assert: Verify data in Oracle
            await using var verifyConnection = new OracleConnection(_oracle.GetConnectionString());
            await verifyConnection.OpenAsync(TestContext.Current.CancellationToken);
            
            await using var countCmd = verifyConnection.CreateCommand();
            countCmd.CommandText = $"SELECT COUNT(*) FROM \"{targetTable}\"";
            var count = Convert.ToInt32(await countCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
            Assert.Equal(4, count); // 4 rows from test-data.json
            
            // Verify sample data integrity
            await using var selectCmd = verifyConnection.CreateCommand();
            selectCmd.CommandText = $"SELECT \"Name\", \"Score\" FROM \"{targetTable}\" WHERE \"Id\" = 1";
            await using var resultReader = await selectCmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await resultReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal("Alice", resultReader.GetString(0));
            Assert.Equal(95.50m, resultReader.GetDecimal(1));
        }
        finally
        {
            if (File.Exists(duckDbPath))
                File.Delete(duckDbPath);
        }
    }
}
