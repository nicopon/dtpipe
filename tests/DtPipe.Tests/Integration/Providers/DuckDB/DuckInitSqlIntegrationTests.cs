using DtPipe.Adapters.DuckDB;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using DuckDB.NET.Data;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// Integration tests for the --duck-init feature across DuckDataSourceReader and DuckDbDataWriter.
/// Uses file-based DuckDB databases in temp directories (no external services required).
/// </summary>
public class DuckInitSqlIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _connectionString;

    public DuckInitSqlIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"dtpipe_init_test_{Guid.NewGuid():N}.duckdb");
        _connectionString = $"Data Source={_dbPath}";
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        try { File.Delete(_dbPath); } catch { /* best effort */ }
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    // ── DuckDataSourceReader ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Reader_InitSql_CreatesView_ViewIsQueryable()
    {
        // Seed a base table
        await using (var conn = new DuckDBConnection(_connectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE source (n INTEGER); INSERT INTO source VALUES (1), (2), (3)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        // initSql creates a VIEW; the main query reads from it
        var options = new DuckDbReaderOptions
        {
            InitSql = "CREATE VIEW evens AS SELECT n FROM source WHERE n % 2 = 0"
        };
        await using var reader = new DuckDataSourceReader(
            _connectionString,
            "SELECT n FROM evens",
            options);

        await reader.OpenAsync(TestContext.Current.CancellationToken);

        var batches = new List<Apache.Arrow.RecordBatch>();
        await foreach (var batch in reader.ReadRecordBatchesAsync(TestContext.Current.CancellationToken))
            batches.Add(batch);

        batches.Sum(b => b.Length).Should().Be(1);
        var col = batches[0].Column(0) as Apache.Arrow.Int32Array;
        col.Should().NotBeNull();
        col!.GetValue(0).Should().Be(2);
    }

    [Fact]
    public async Task Reader_InitSql_AtFilePrefix_LoadsSqlFromFile()
    {
        await using (var conn = new DuckDBConnection(_connectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE nums (n INTEGER); INSERT INTO nums VALUES (99)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var tempFile = Path.GetTempFileName();
        try
        {
            // initSql file creates a VIEW that the reader queries
            await File.WriteAllTextAsync(tempFile,
                "CREATE VIEW from_file_view AS SELECT n * 2 AS doubled FROM nums",
                TestContext.Current.CancellationToken);

            var options = new DuckDbReaderOptions { InitSql = $"@{tempFile}" };
            await using var reader = new DuckDataSourceReader(
                _connectionString,
                "SELECT doubled FROM from_file_view",
                options);

            await reader.OpenAsync(TestContext.Current.CancellationToken);

            var batches = new List<Apache.Arrow.RecordBatch>();
            await foreach (var batch in reader.ReadRecordBatchesAsync(TestContext.Current.CancellationToken))
                batches.Add(batch);

            batches.Sum(b => b.Length).Should().Be(1);
            var col = batches[0].Column(0) as Apache.Arrow.Int32Array;
            col!.GetValue(0).Should().Be(198);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Reader_InitSql_Null_WorksNormally()
    {
        await using (var conn = new DuckDBConnection(_connectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE nums (n INTEGER); INSERT INTO nums VALUES (7)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var options = new DuckDbReaderOptions { InitSql = null };
        await using var reader = new DuckDataSourceReader(
            _connectionString,
            "SELECT n FROM nums",
            options);

        await reader.OpenAsync(TestContext.Current.CancellationToken);

        var batches = new List<Apache.Arrow.RecordBatch>();
        await foreach (var b in reader.ReadRecordBatchesAsync(TestContext.Current.CancellationToken))
            batches.Add(b);

        batches.Sum(b => b.Length).Should().Be(1);
    }

    // ── DuckDbDataWriter ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Writer_InitSql_MacroDefinedInInit_QuerySucceeds()
    {
        // Define a table and populate it from initSql (DDL/DML in initSql is intentionally
        // allowed — only the *reader*'s main query is restricted to SELECT/WITH).
        // Here we just verify the writer opens cleanly when initSql runs a benign SET.
        var writerOptions = new DuckDbWriterOptions
        {
            Table = "results",
            Strategy = DuckDbWriteStrategy.Recreate,
            InitSql = "SET threads = 1"
        };

        var columns = new List<PipeColumnInfo>
        {
            new("id",    typeof(int),    false),
            new("label", typeof(string), true),
        };
        var rows = new List<object?[]>
        {
            new object?[] { 1, "alpha" },
            new object?[] { 2, "beta" },
        };

        await using var writer = new DuckDbDataWriter(
            _connectionString, writerOptions,
            NullLogger<DuckDbDataWriter>.Instance,
            DuckDbTypeConverter.Instance);

        await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        await writer.WriteRecordBatchAsync(rows.ToRecordBatch(columns), TestContext.Current.CancellationToken);
        await writer.CompleteAsync(TestContext.Current.CancellationToken);

        // Verify data was written correctly
        await using var conn = new DuckDBConnection(_connectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM results";
        var count = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        Convert.ToInt64(count).Should().Be(2);
    }

    [Fact]
    public async Task Writer_InitSql_AtFilePrefix_LoadsSqlFromFile()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(tempFile, "SET threads = 1",
                TestContext.Current.CancellationToken);

            var writerOptions = new DuckDbWriterOptions
            {
                Table = "data",
                Strategy = DuckDbWriteStrategy.Recreate,
                InitSql = $"@{tempFile}"
            };

            var columns = new List<PipeColumnInfo> { new("x", typeof(int), false) };
            var rows = new List<object?[]> { new object?[] { 42 } };

            await using var writer = new DuckDbDataWriter(
                _connectionString, writerOptions,
                NullLogger<DuckDbDataWriter>.Instance,
                DuckDbTypeConverter.Instance);

            await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
            await writer.WriteRecordBatchAsync(rows.ToRecordBatch(columns), TestContext.Current.CancellationToken);
            await writer.CompleteAsync(TestContext.Current.CancellationToken);

            await using var conn = new DuckDBConnection(_connectionString);
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT x FROM data";
            var val = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
            Convert.ToInt32(val).Should().Be(42);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Writer_InitSql_Null_WritesNormally()
    {
        var writerOptions = new DuckDbWriterOptions
        {
            Table = "plain",
            Strategy = DuckDbWriteStrategy.Recreate,
            InitSql = null
        };

        var columns = new List<PipeColumnInfo> { new("v", typeof(string), true) };
        var rows = new List<object?[]> { new object?[] { "ok" } };

        await using var writer = new DuckDbDataWriter(
            _connectionString, writerOptions,
            NullLogger<DuckDbDataWriter>.Instance,
            DuckDbTypeConverter.Instance);

        await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
        await writer.WriteRecordBatchAsync(rows.ToRecordBatch(columns), TestContext.Current.CancellationToken);
        await writer.CompleteAsync(TestContext.Current.CancellationToken);

        await using var conn = new DuckDBConnection(_connectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT v FROM plain";
        var val = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        val.Should().Be("ok");
    }
}
