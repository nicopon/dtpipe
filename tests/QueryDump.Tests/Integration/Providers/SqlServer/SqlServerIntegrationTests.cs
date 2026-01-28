using Testcontainers.MsSql;
using Xunit;
using Microsoft.Data.SqlClient;
using QueryDump.Adapters.SqlServer;
using QueryDump.Adapters;
using QueryDump.Adapters.Parquet;
using QueryDump.Tests.Helpers;

namespace QueryDump.Tests;

/// <summary>
/// Integration tests using SQL Server Testcontainers.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class SqlServerIntegrationTests : IAsyncLifetime
{
    private MsSqlContainer? _sqlServer;

    public async ValueTask InitializeAsync()
    {
        if (!DockerHelper.IsAvailable())
        {
            return;
        }

        try
        {
            _sqlServer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
            await _sqlServer.StartAsync();
            
            await using var connection = new SqlConnection(_sqlServer.GetConnectionString());
            await connection.OpenAsync();
            
            // Use Seeder for DDL and Data
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "test_data");
            await cmd.ExecuteNonQueryAsync();

            await TestDataSeeder.SeedAsync(connection, "test_data");
        }
        catch (Exception)
        {
            _sqlServer = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_sqlServer is not null)
        {
            await _sqlServer.DisposeAsync();
        }
    }

    [Fact]
    public async Task SqlServerStreamReader_ReadsAllRows()
    {
        if (!DockerHelper.IsAvailable() || _sqlServer is null) return;

        // Arrange
        var connectionString = _sqlServer.GetConnectionString();
        
        // Act
        await using var reader = new SqlServerStreamReader(
            connectionString, 
            "SELECT id, name FROM test_data ORDER BY id",
            new SqlServerReaderOptions());
        
        await reader.OpenAsync(TestContext.Current.CancellationToken);
        
        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(10, TestContext.Current.CancellationToken))
        {
            for(int i = 0; i < batch.Length; i++)
            {
                rows.Add(batch.Span[i]);
            }
        }
        
        // Assert
        Assert.Equal(4, rows.Count); // 4 records in test-data.json
        Assert.Equal(2, reader.Columns!.Count); // 2 columns (id, name)
        Assert.Equal("id", reader.Columns[0].Name);
        
        // Specific data validation (diverse types)
        var alice = rows.First(r => r[0]?.ToString() == "1");
        Assert.Equal("Alice", alice[1]); // Name
    }

    [Fact]
    public async Task ParquetWriter_CreatesValidFile_FromSqlServer()
    {
        if (!DockerHelper.IsAvailable() || _sqlServer is null) return;

        // Arrange
        var connectionString = _sqlServer.GetConnectionString();
        var outputPath = Path.Combine(Path.GetTempPath(), $"test_sql_{Guid.NewGuid()}.parquet");
        
        try
        {
            // Act
            await using var reader = new SqlServerStreamReader(
                connectionString, 
                "SELECT * FROM test_data ORDER BY Id",
                new SqlServerReaderOptions());
            
            await reader.OpenAsync(TestContext.Current.CancellationToken);
            
            await using var writer = new ParquetDataWriter(outputPath);
            await writer.InitializeAsync(reader.Columns!, TestContext.Current.CancellationToken);
            
            var rows = new List<object?[]>();
            await foreach (var batchChunk in reader.ReadBatchesAsync(100, TestContext.Current.CancellationToken))
            {
                 for(int i = 0; i < batchChunk.Length; i++)
                {
                    rows.Add(batchChunk.Span[i]);
                }
            }
            
            await writer.WriteBatchAsync(rows, TestContext.Current.CancellationToken);
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
    public async Task SqlServerDataWriter_MixedOrder_MapsCorrectly()
    {
        if (!DockerHelper.IsAvailable() || _sqlServer is null) return;

        // Arrange
        var connectionString = _sqlServer.GetConnectionString();
        var tableName = "MixedOrderTest";

        // 1. Manually create table with mixed order: Score (DECIMAL), Name (NVARCHAR), Id (INT)
        // Source will be: Id, Name, Score
        await using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"CREATE TABLE {tableName} (Score DECIMAL(18,2), Name NVARCHAR(100), Id INT)";
            await cmd.ExecuteNonQueryAsync();
        }

        // 2. Setup Source Data
        var columns = new List<QueryDump.Core.Models.ColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), true),
            new("Score", typeof(decimal), false)
        };

        var row1 = new object?[] { 1, "Alice", 95.5m };
        var row2 = new object?[] { 2, "Bob", 80.0m };
        var batch = new List<object?[]> { row1, row2 };

        var writerOptions = new SqlServerWriterOptions
        {
            Table = tableName,
            Strategy = SqlServerWriteStrategy.Truncate
        };

        // Act
        await using var writer = new SqlServerDataWriter(connectionString, writerOptions);
        await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        await writer.WriteBatchAsync(batch, TestContext.Current.CancellationToken);
        await writer.CompleteAsync(TestContext.Current.CancellationToken);

        // Assert
        await using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT Id, Name, Score FROM {tableName} ORDER BY Id";
            using var reader = await cmd.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetInt32(0)); // Id
            Assert.Equal("Alice", reader.GetString(1)); // Name
            Assert.Equal(95.5m, reader.GetDecimal(2)); // Score
            
            Assert.True(await reader.ReadAsync());
            Assert.Equal(2, reader.GetInt32(0));
            Assert.Equal("Bob", reader.GetString(1));
            Assert.Equal(80.0m, reader.GetDecimal(2));
        }
    }
}
