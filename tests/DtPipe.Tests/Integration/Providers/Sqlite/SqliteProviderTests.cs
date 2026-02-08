using FluentAssertions;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Adapters.Sqlite;
using Xunit;

namespace DtPipe.Tests;

public class SqliteProviderTests : IAsyncLifetime
{
    private string _testDbPath = null!;
    private string _outputDbPath = null!;
    private string _connectionString = null!;
    private string _outputConnectionString = null!;

    public ValueTask InitializeAsync()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.sqlite");
        _outputDbPath = Path.Combine(Path.GetTempPath(), $"output_{Guid.NewGuid()}.sqlite");
        _connectionString = $"Data Source={_testDbPath}";
        _outputConnectionString = $"Data Source={_outputDbPath}";
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (File.Exists(_testDbPath)) File.Delete(_testDbPath);
        if (File.Exists(_outputDbPath)) File.Delete(_outputDbPath);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task SqliteReader_ShouldReadData()
    {
        // Arrange: Create test database
        await using (var conn = new SqliteConnection(_connectionString))
        {
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE Users (Id INTEGER, Name TEXT, Age INTEGER)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "INSERT INTO Users VALUES (1, 'Alice', 30), (2, 'Bob', 25)";
            await cmd.ExecuteNonQueryAsync();
        }

        // Act
        var reader = new SqliteStreamReader(_connectionString, "SELECT * FROM Users");
        await reader.OpenAsync();
        var columns = reader.Columns;
        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(100))
        {
            foreach (var row in batch.ToArray())
            {
                rows.Add(row);
            }
        }
        await reader.DisposeAsync();

        // Assert
        columns.Should().HaveCount(3);
        columns![0].Name.Should().Be("Id");
        columns[1].Name.Should().Be("Name");
        columns[2].Name.Should().Be("Age");
        rows.Should().HaveCount(2);
        rows[0][1].Should().Be("Alice");
        rows[1][1].Should().Be("Bob");
    }

    [Fact]
    public async Task SqliteWriter_ShouldWriteData()
    {
        // Arrange
        var registry = new OptionsRegistry();
        registry.Register(new SqliteWriterOptions { Table = "Export", Strategy = SqliteWriteStrategy.Recreate });

        var columns = new List<PipeColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), true),
            new("Score", typeof(double), true)
        };

        var rows = new List<object?[]>
        {
            new object?[] { 1, "Alice", 95.5 },
            new object?[] { 2, "Bob", 87.3 },
            new object?[] { 3, "Charlie", null }
        };

        // Act
        var writer = new SqliteDataWriter(_outputConnectionString, registry);
        await writer.InitializeAsync(columns);
        await writer.WriteBatchAsync(rows);
        await writer.CompleteAsync();
        await writer.DisposeAsync();

        // Assert: Read back
        await using var conn = new SqliteConnection(_outputConnectionString);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Export";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(3);
    }

    [Fact]
    public async Task SqliteRoundTrip_ShouldPreserveData()
    {
        // Arrange: Create source database
        await using (var conn = new SqliteConnection(_connectionString))
        {
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE Source (Id INTEGER, Value TEXT)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "INSERT INTO Source VALUES (1, 'Test1'), (2, 'Test2'), (3, 'Test3')";
            await cmd.ExecuteNonQueryAsync();
        }

        var registry = new OptionsRegistry();
        registry.Register(new SqliteWriterOptions { Table = "Target", Strategy = SqliteWriteStrategy.Recreate });

        // Act: Read from source
        var reader = new SqliteStreamReader(_connectionString, "SELECT * FROM Source");
        await reader.OpenAsync();
        var columns = reader.Columns!;

        // Write to target
        var writer = new SqliteDataWriter(_outputConnectionString, registry);
        await writer.InitializeAsync(columns);

        await foreach (var batch in reader.ReadBatchesAsync(100))
        {
            await writer.WriteBatchAsync(batch.ToArray().ToList());
        }

        await writer.CompleteAsync();
        await writer.DisposeAsync();
        await reader.DisposeAsync();

        // Assert: Verify target
        await using var verifyConn = new SqliteConnection(_outputConnectionString);
        await verifyConn.OpenAsync();
        using var verifyCmd = verifyConn.CreateCommand();
        verifyCmd.CommandText = "SELECT * FROM Target ORDER BY Id";
        await using var result = await verifyCmd.ExecuteReaderAsync();
        
        var readRows = new List<(long Id, string Value)>();
        while (await result.ReadAsync())
        {
            readRows.Add((result.GetInt64(0), result.GetString(1)));
        }

        readRows.Should().HaveCount(3);
        readRows[0].Value.Should().Be("Test1");
        readRows[2].Value.Should().Be("Test3");
    }

    [Fact]
    public void SqliteConnectionHelper_ShouldDetectSqlitePaths()
    {
        SqliteConnectionHelper.CanHandle("test.sqlite").Should().BeTrue();
        SqliteConnectionHelper.CanHandle("test.sqlite3").Should().BeTrue();
        SqliteConnectionHelper.CanHandle("test.db").Should().BeFalse(); // .db is DuckDB by default
        SqliteConnectionHelper.CanHandle("connection").Should().BeFalse();
    }
    [Fact]
    public async Task SqliteDataWriter_MixedOrder_MapsCorrectly()
    {
        // Arrange
        var tableName = "MixedOrderTest";
        
        // 1. Manually create table with mixed order: Score (REAL), Name (TEXT), Id (INTEGER)
        await using (var connection = new SqliteConnection(_outputConnectionString))
        {
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"CREATE TABLE {tableName} (Score REAL, Name TEXT, Id INTEGER)";
            await cmd.ExecuteNonQueryAsync();
        }

        // 2. Setup Source Data
        var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), true),
            new("Score", typeof(double), false)
        };

        var row1 = new object?[] { 1, "Alice", 95.5 };
        var row2 = new object?[] { 2, "Bob", 80.0 };
        var batch = new List<object?[]> { row1, row2 };

        var registry = new OptionsRegistry();
        registry.Register(new SqliteWriterOptions 
        { 
            Table = tableName, 
            Strategy = SqliteWriteStrategy.Append 
        });

        // Act
        await using var writer = new SqliteDataWriter(_outputConnectionString, registry);
        await writer.InitializeAsync(columns, CancellationToken.None);
        await writer.WriteBatchAsync(batch, CancellationToken.None);
        await writer.CompleteAsync(CancellationToken.None);
        
        // Assert
        await using (var connection = new SqliteConnection(_outputConnectionString))
        {
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT Id, Name, Score FROM {tableName} ORDER BY Id";
            using var reader = await cmd.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetInt32(0)); // Id
            Assert.Equal("Alice", reader.GetString(1)); // Name
            Assert.Equal(95.5, reader.GetDouble(2)); // Score
            
            Assert.True(await reader.ReadAsync());
            Assert.Equal(2, reader.GetInt32(0));
            Assert.Equal("Bob", reader.GetString(1));
            Assert.Equal(80.0, reader.GetDouble(2));
        }
    }
    [Fact]
    public async Task SqliteDataWriter_DeleteThenInsert_ShouldClearAndInsert()
    {
        // Arrange
        var tableName = "DeleteInsertTest";
        
        // 1. Create table and insert initial data
        await using (var connection = new SqliteConnection(_outputConnectionString))
        {
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"CREATE TABLE {tableName} (Id INTEGER, Name TEXT)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = $"INSERT INTO {tableName} VALUES (999, 'OldData')";
            await cmd.ExecuteNonQueryAsync();
        }

        var registry = new OptionsRegistry();
        registry.Register(new SqliteWriterOptions { Table = tableName, Strategy = SqliteWriteStrategy.DeleteThenInsert });

        var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { 1, "NewData" } };

        // Act
        await using var writer = new SqliteDataWriter(_outputConnectionString, registry);
        await writer.InitializeAsync(columns, CancellationToken.None);
        await writer.WriteBatchAsync(rows, CancellationToken.None);
        await writer.CompleteAsync(CancellationToken.None);
        
        // Assert
        await using (var connection = new SqliteConnection(_outputConnectionString))
        {
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand(); // Check count matches new data only
            cmd.CommandText = $"SELECT COUNT(*) FROM {tableName}";
            var count = (long)(await cmd.ExecuteScalarAsync())!;
            count.Should().Be(1);

            using var cmd2 = connection.CreateCommand();
            cmd2.CommandText = $"SELECT Name FROM {tableName}";
            var name = (string)(await cmd2.ExecuteScalarAsync())!;
            name.Should().Be("NewData");
        }
    }

    [Fact]
    public async Task SqliteDataWriter_Recreate_PreservesNativeStructure()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"test_recreate_p_{Guid.NewGuid():N}.sqlite");
        var connectionString = $"Data Source={dbPath}";
        var tableNameRaw = "TestRecreateEnh";

        try
        {
            // 1. Manually create table with specific structure:
            // - Code: VARCHAR(10) (Explicit length)
            // - Score: DECIMAL(10,5) (Specific precision)
            // - "Memo Text": TEXT (Quoted)
            // - BlobData: BLOB
            await using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $@"
                    CREATE TABLE {tableNameRaw} (
                        Code VARCHAR(10) NOT NULL, 
                        Score DECIMAL(10,5), 
                        ""Memo Text"" TEXT,
                        BlobData BLOB,
                        PRIMARY KEY (Code)
                    )";
                await cmd.ExecuteNonQueryAsync();
                
                cmd.CommandText = $"INSERT INTO {tableNameRaw} VALUES ('OLD', 10.5, 'OldMemo', X'AA')";
                await cmd.ExecuteNonQueryAsync();
            }

            var registry = new OptionsRegistry();
            registry.Register(new SqliteWriterOptions { Table = tableNameRaw, Strategy = SqliteWriteStrategy.Recreate });

            var columns = new List<PipeColumnInfo> 
            { 
                new("Code", typeof(string), true), 
                new("Score", typeof(decimal), false),
                new("Memo Text", typeof(string), true),
                new("BlobData", typeof(byte[]), true)
            };
            
            var batch = new List<object?[]> { new object?[] { "NEW", 99.12345m, "NewMemo", new byte[] { 0xBB } } };

            // Act
            await using var writer = new SqliteDataWriter(connectionString, registry);
            await writer.InitializeAsync(columns);
            await writer.WriteBatchAsync(batch);
            await writer.CompleteAsync();

            // Assert
            await using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();

                // Check Data
                using var cmd = connection.CreateCommand();
                cmd.CommandText = $"SELECT Code, Score, \"Memo Text\", BlobData FROM {tableNameRaw}";
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    Assert.True(await reader.ReadAsync());
                    Assert.Equal("NEW", reader.GetString(0));
                    Assert.Equal(99.12345m, reader.GetDecimal(1));
                    Assert.Equal("NewMemo", reader.GetString(2));
                    var blob = (byte[])reader.GetValue(3);
                    Assert.Equal(0xBB, blob[0]);
                }

                // Check Metadata (using PRAGMA table_info)
                using var metaCmd = connection.CreateCommand();
                metaCmd.CommandText = $"PRAGMA table_info('{tableNameRaw}')";
                
                var types = new Dictionary<string, string>();
                using (var metaReader = await metaCmd.ExecuteReaderAsync())
                {
                    while(await metaReader.ReadAsync())
                    {
                        types[metaReader.GetString(1)] = metaReader.GetString(2);
                    }
                }
                
                Assert.Contains("VARCHAR(10)", types["Code"]);
                Assert.Contains("DECIMAL(10,5)", types["Score"]);
                Assert.Contains("TEXT", types["Memo Text"]);
                Assert.Contains("BLOB", types["BlobData"]);
            }
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }
}
