using DtPipe.Adapters.PostgreSQL;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DtPipe.Tests;

[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class PostgreSqlIntegrationTests : IAsyncLifetime
{
    private readonly DtPipe.Tests.Fixtures.GlobalDatabaseFixture _fixture;
    private string? _connectionString;

    public PostgreSqlIntegrationTests(DtPipe.Tests.Fixtures.GlobalDatabaseFixture fixture)
    {
        _fixture = fixture;
    }

    public async ValueTask InitializeAsync()
    {
        _connectionString = _fixture.PostgresConnectionString;

        if (_connectionString == null) return;

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        // Ensure clean state for each test method
        cmd.CommandText = "DROP TABLE IF EXISTS test_data; " + TestDataSeeder.GenerateTableDDL(connection, "test_data");
        await cmd.ExecuteNonQueryAsync();

        await TestDataSeeder.SeedAsync(connection, "test_data");
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task PostgreSqlReader_ReadsAllRows()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        var connectionString = _connectionString;

        await using var reader = new PostgreSqlReader(
            connectionString,
            "SELECT * FROM test_data ORDER BY Id", // Unquoted identifier for case-insensitive match (Postgres defaults to lowercase)
            timeout: 30);

        await reader.OpenAsync();

        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(10))
        {
            for (int i = 0; i < batch.Length; i++)
            {
                rows.Add(batch.Span[i]);
            }
        }

        Assert.Equal(4, rows.Count);
        Assert.Equal(7, reader.Columns!.Count);
        // Postgres returns lower case column names usually unless quoted in creation?
        // TestDataSeeder uses unquoted CREATE TABLE names, so Postgres lowercases them.
        Assert.Equal("id", reader.Columns[0].Name.ToLower());

        var alice = rows.First(r => r[0]?.ToString() == "1");
        // Check boolean
        Assert.Equal(true, alice[3]);
        // Check numeric
        Assert.Equal(95.50m, alice[4]);
    }

    [Fact]
    public async Task PostgreSqlReader_ReadsJsonbAndJsonpathWithoutTheBinaryVersionByte()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        const string table = "zz_reader_jsonb_prefix";
        await using var setup = new NpgsqlConnection(_connectionString);
        await setup.OpenAsync();
        try
        {
            await using (var ddl = setup.CreateCommand())
            {
                ddl.CommandText =
                    $"DROP TABLE IF EXISTS {table}; " +
                    $"CREATE TABLE {table}(jb jsonb, jbn jsonb, ja jsonb[], jp jsonpath); " +
                    $"INSERT INTO {table} VALUES ('{{\"a\": 1,   \"b\": [1, 2]}}', NULL, ARRAY['{{\"k\": 1}}']::jsonb[], '$.a')";
                await ddl.ExecuteNonQueryAsync();
            }

            string expectedJsonb, expectedElement, expectedPath;
            await using (var q = setup.CreateCommand())
            {
                q.CommandText = $"SELECT jb::text, ja[1]::text, jp::text FROM {table}";
                await using var r = await q.ExecuteReaderAsync();
                Assert.True(await r.ReadAsync());
                (expectedJsonb, expectedElement, expectedPath) = (r.GetString(0), r.GetString(1), r.GetString(2));
            }

            await using var reader = new PostgreSqlReader(_connectionString, $"SELECT jb, jbn, ja, jp FROM {table}", timeout: 30);
            await reader.OpenAsync();

            var rows = new List<object?[]>();
            await foreach (var batch in reader.ReadBatchesAsync(10))
                for (int i = 0; i < batch.Length; i++) rows.Add(batch.Span[i]);

            var row = Assert.Single(rows);
            Assert.Equal(expectedJsonb, row[0]);
            Assert.Null(row[1]);
            Assert.Equal(expectedElement, Assert.Single(((System.Collections.IEnumerable)row[2]!).Cast<object?>()));
            Assert.Equal(expectedPath, row[3]);
        }
        finally
        {
            await using var drop = setup.CreateCommand();
            drop.CommandText = $"DROP TABLE IF EXISTS {table}";
            await drop.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task PostgreSqlWriter_WritesAJsonlObjectOfVaryingShapeKeyForKey()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        const string table = "zz_jsonl_varying_shape";
        var events = JsonLShapeData.Events(120).ToList();
        var source = Path.Combine(Path.GetTempPath(), $"jsonl_shape_{Guid.NewGuid()}.jsonl");
        await File.WriteAllLinesAsync(source, events);
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();
        try
        {
            var exit = await DtPipe.Program.Main(["-i", $"jsonl:{source}", "-o", $"pg:{_connectionString}", "--table", table, "--strategy", "Recreate", "--no-stats"]);
            Assert.Equal(0, exit);

            var payloads = new Dictionary<long, string>();
            await using (var query = connection.CreateCommand())
            {
                query.CommandText = $"SELECT id, payload::text FROM {table}";
                await using var reader = await query.ExecuteReaderAsync();
                while (await reader.ReadAsync()) payloads[reader.GetInt64(0)] = reader.GetString(1);
            }

            Assert.Equal(events.Count, payloads.Count);
            Assert.Empty(JsonLShapeData.Differences(events, payloads));
        }
        finally
        {
            await using var drop = connection.CreateCommand();
            drop.CommandText = $"DROP TABLE IF EXISTS {table}";
            await drop.ExecuteNonQueryAsync();
            try { File.Delete(source); } catch { /* best effort */ }
        }
    }

    [Theory]
    [InlineData("timestamp(6)", "2026-01-02 00:00:00.654321")]
    [InlineData("timestamptz", "2026-01-02 00:00:00.654321+00")]
    public async Task Cursor_StrictBoundAndAppend_AddNoRowAfterTheFirstRun(string columnType, string latest)
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        const string table = "zz_cursor_microseconds";
        var dir = Path.Combine(Path.GetTempPath(), $"cursor_us_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var (state, target) = (Path.Combine(dir, "c.state"), Path.Combine(dir, "t.duckdb"));
        await using var setup = new NpgsqlConnection(_connectionString);
        await setup.OpenAsync();
        try
        {
            await using (var create = setup.CreateCommand())
            {
                create.CommandText = $"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (id int, updated_at {columnType}); "
                    + $"INSERT INTO {table} VALUES (1, '2026-01-01 00:00:00.123456+00'), (2, '{latest}')";
                await create.ExecuteNonQueryAsync();
            }

            var query = $"SELECT id, updated_at FROM {table} WHERE updated_at > '${{{{cursor://{state}|1970-01-01T00:00:00.000}}}}'";
            var counts = new List<long>();
            for (int run = 0; run < 3; run++)
            {
                Assert.Equal(0, await DtPipe.Program.Main(["-i", $"pg:{_connectionString}", "--query", query, "-o", $"duck:{target}",
                    "--table", "t", "--strategy", "Append", "--cursor", "updated_at", "--state", state, "--no-stats"]));
                await using var duck = new DuckDB.NET.Data.DuckDBConnection($"Data Source={target}");
                await duck.OpenAsync();
                await using var count = duck.CreateCommand();
                count.CommandText = "SELECT count(*) FROM t";
                counts.Add(Convert.ToInt64(await count.ExecuteScalarAsync()));
            }

            Assert.Equal([2L, 2L, 2L], counts);
        }
        finally
        {
            await using var drop = setup.CreateCommand();
            drop.CommandText = $"DROP TABLE IF EXISTS {table}";
            await drop.ExecuteNonQueryAsync();
            try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task PostgreSqlWriter_WritesRows()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        var connectionString = _connectionString;
        var targetTable = "test_export";

        // Options
        var options = new PostgreSqlWriterOptions
        {
            Table = targetTable,
            Strategy = PostgreSqlWriteStrategy.Truncate
        };

        // Prepare Data
        var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true),
            new("created", typeof(DateTime), true)
        };

        var data = new List<object?[]>
        {
            new object?[] { 1, "Test 1", DateTime.UtcNow },
            new object?[] { 2, "Test 2", DateTime.UtcNow.AddDays(-1) }
        };

        // Act
        await using var writer = new PostgreSqlDataWriter(connectionString, options, NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance);
        await writer.InitializeAsync(columns);
        await writer.WriteBatchAsync(data);
        await writer.CompleteAsync();

        // Assert
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {targetTable}", connection);
        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());

        Assert.Equal(2, count);
    }
    [Fact]
    public async Task PostgreSqlDataWriter_MixedOrder_MapsCorrectly()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        // Arrange
        var connectionString = _connectionString;
        var tableName = "mixed_order_test";

        // 1. Manually create table with DIFFERENT column order than source data
        // Table: (score, name, id) vs Source: (id, name, score)
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {tableName}; CREATE TABLE {tableName} (score NUMERIC, name TEXT, id INT)";
            await cmd.ExecuteNonQueryAsync();
        }

        // 2. Setup Source Data
        var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true),
            new("score", typeof(decimal), false)
        };

        var row1 = new object?[] { 1, "Alice", 95.5m };
        var row2 = new object?[] { 2, "Bob", 80.0m };
        var batch = new List<object?[]> { row1, row2 };

        var writerOptions = new PostgreSqlWriterOptions
        {
            Table = tableName,
            Strategy = PostgreSqlWriteStrategy.Append
        };

        // Act
        await using var writer = new PostgreSqlDataWriter(connectionString, writerOptions, NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance);
        await writer.InitializeAsync(columns); // Will find table and append
        await writer.WriteBatchAsync(batch);
        await writer.CompleteAsync();

        // Assert
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT id, name, score FROM {tableName} ORDER BY id";
            await using var reader = await cmd.ExecuteReaderAsync();

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
    [Fact]
    public async Task PostgreSqlWriter_DeleteThenInsert_ClearsTable()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        var connectionString = _connectionString;
        var targetTable = "test_delete_insert";

        // 1. Create table and insert initial data
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {targetTable}; CREATE TABLE {targetTable} (id INT, name TEXT)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = $"INSERT INTO {targetTable} VALUES (999, 'OldData')";
            await cmd.ExecuteNonQueryAsync();
        }

        var options = new PostgreSqlWriterOptions { Table = targetTable, Strategy = PostgreSqlWriteStrategy.DeleteThenInsert };
        var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
        var data = new List<object?[]> { new object?[] { 1, "NewData" } };

        // Act
        await using var writer = new PostgreSqlDataWriter(connectionString, options, NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance);
        await writer.InitializeAsync(columns);
        await writer.WriteBatchAsync(data);
        await writer.CompleteAsync();

        // Assert
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {targetTable}", connection);
            var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            Assert.Equal(1, count); // Only new data

            await using var checkCmd = new NpgsqlCommand($"SELECT id FROM {targetTable}", connection);
            var id = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
            Assert.Equal(1, id);
        }
    }

    [Fact]
    public async Task PostgreSqlWriter_Recreate_DropsAndCreatesTable()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        var connectionString = _connectionString;
        var targetTable = "test_recreate";

        // 1. Create table with incompatible schema (Name as INT)
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS {targetTable}; CREATE TABLE {targetTable} (id INT, name TEXT)"; // Compatible with String
            await cmd.ExecuteNonQueryAsync();
        }

        var options = new PostgreSqlWriterOptions { Table = targetTable, Strategy = PostgreSqlWriteStrategy.Recreate };
        // Source defines Name as String
        var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
        var data = new List<object?[]> { new object?[] { 1, "NewData" } };

        // Act
        await using var writer = new PostgreSqlDataWriter(connectionString, options, NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance);
        await writer.InitializeAsync(columns); // Should Drop and Recreate with correct schema (Name TEXT)
        await writer.WriteBatchAsync(data);
        await writer.CompleteAsync();

        // Assert
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            // Verify data exists (means write succeeded)
            await using var cmd = new NpgsqlCommand($"SELECT name FROM {targetTable}", connection);
            var name = await cmd.ExecuteScalarAsync() as string;
            Assert.Equal("NewData", name);
        }
    }

    [Fact]
    public async Task PostgreSqlWriter_Recreate_RebuildsFromTheSourceSchema()
    {
        if (!DockerHelper.IsAvailable() || _connectionString is null) return;

        var connectionString = _connectionString;
        var tableNameRaw = $"test_recreate_p_{Guid.NewGuid():N}".Substring(0, 30); // Lowercase unquoted

        // 1. Manually create table with specific structure:
        // - code: CHAR(10) (Fixed length, unquoted)
        // - "MixedScore": NUMERIC(10,5) (Quoted, mixed case)
        // - description: TEXT
        // - flags: VARCHAR(10) (String representation of bits)
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
                CREATE TABLE {tableNameRaw} (
                    code CHAR(10) NOT NULL,
                    ""MixedScore"" NUMERIC(10,5),
                    description TEXT,
                    flags VARCHAR(10),
                    extra INTEGER,
                    PRIMARY KEY (code)
                )";
            await cmd.ExecuteNonQueryAsync();

            cmd.CommandText = $"INSERT INTO {tableNameRaw} (code, \"MixedScore\", description, flags, extra) VALUES ('OLD', 10.5, 'OldDesc', B'101', 5)";
            await cmd.ExecuteNonQueryAsync();
        }

        var options = new PostgreSqlWriterOptions { Table = tableNameRaw, Strategy = PostgreSqlWriteStrategy.Recreate };

        // Source defines columns generically
        var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
        {
            new("code", typeof(string), true),
            new("MixedScore", typeof(decimal), false),
            new("description", typeof(string), true),
            new("flags", typeof(string), true)
        };

        var batch = new List<object?[]> { new object?[] { "NEW", 99.12345m, "NewDesc", "010" } }; // "010" string -> BIT(3)

        // Act
        // Recreate drops the table and builds it from the source columns
        await using var writer = new PostgreSqlDataWriter(connectionString, options, NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance);
        await writer.InitializeAsync(columns);
        await writer.WriteBatchAsync(batch);
        await writer.CompleteAsync();

        // Assert
        // Inspect table structure to verify it was rebuilt from the source, not from the old table
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            // Check Data
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT code, MixedScore, description, flags::TEXT FROM {tableNameRaw}"; // Cast flags to text for easy reading
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal("NEW", reader.GetString(0)); // the source says string, not CHAR(10)
                Assert.Equal(99.12345m, reader.GetDecimal(1));
                Assert.Equal("NewDesc", reader.GetString(2));
                Assert.Equal("010", reader.GetString(3));
            }

            // Check Metadata using information_schema
            using var meta = connection.CreateCommand();
            meta.CommandText = $"SELECT column_name, data_type, character_maximum_length, numeric_precision FROM information_schema.columns WHERE table_name = '{tableNameRaw}' ORDER BY column_name";
            var columnsFound = new Dictionary<string, (string Type, object Length, object Precision)>();
            using (var r = await meta.ExecuteReaderAsync())
            {
                while (await r.ReadAsync())
                    columnsFound[r.GetString(0)] = (r.GetString(1), r.GetValue(2), r.GetValue(3));
            }

            // The column the source lacks is gone, and no native width survives.
            Assert.Equal(new[] { "code", "description", "flags", "mixedscore" }, columnsFound.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
            Assert.True(columnsFound["code"].Length is DBNull, "code was CHAR(10); the source column is a string.");
            Assert.True(columnsFound["mixedscore"].Precision is DBNull, "MixedScore was NUMERIC(10,5); the source column is a decimal.");
        }
    }
}
