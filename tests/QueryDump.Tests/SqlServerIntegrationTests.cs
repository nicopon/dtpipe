using Testcontainers.MsSql;
using Xunit;
using Microsoft.Data.SqlClient;
using QueryDump.Providers.SqlServer;
using QueryDump.Writers;
using QueryDump.Writers.Parquet;
using QueryDump.Tests.Helpers;

namespace QueryDump.Tests;

/// <summary>
/// Integration tests using SQL Server Testcontainers.
/// </summary>
[Trait("Category", "Integration")]
public class SqlServerIntegrationTests : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlServer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();

    public async ValueTask InitializeAsync()
    {
        await _sqlServer.StartAsync();
        
        await using var connection = new SqlConnection(_sqlServer.GetConnectionString());
        await connection.OpenAsync();
        
        // Use Seeder for DDL and Data
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "test_data");
        await cmd.ExecuteNonQueryAsync();

        await TestDataSeeder.SeedAsync(connection, "test_data");
    }

    public async ValueTask DisposeAsync()
    {
        await _sqlServer.DisposeAsync();
    }

    [Fact]
    public async Task SqlServerStreamReader_ReadsAllRows()
    {
        // Arrange
        var connectionString = _sqlServer.GetConnectionString();
        
        // Act
        await using var reader = new SqlServerStreamReader(
            connectionString, 
            "SELECT * FROM test_data ORDER BY Id",
            new SqlServerOptions());
        
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
        Assert.Equal(7, reader.Columns!.Count); // 7 columns
        Assert.Equal("Id", reader.Columns[0].Name);
        
        // Specific data validation (diverse types)
        var alice = rows.First(r => r[0]?.ToString() == "1");
        Assert.Equal(true, alice[3]); // IsActive (bool/bit)
        Assert.Equal(95.50m, alice[4]); // Score (decimal)
    }

    [Fact]
    public async Task ParquetWriter_CreatesValidFile_FromSqlServer()
    {
        // Arrange
        var connectionString = _sqlServer.GetConnectionString();
        var outputPath = Path.Combine(Path.GetTempPath(), $"test_sql_{Guid.NewGuid()}.parquet");
        
        try
        {
            // Act
            await using var reader = new SqlServerStreamReader(
                connectionString, 
                "SELECT * FROM test_data ORDER BY Id",
                new SqlServerOptions());
            
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
}
