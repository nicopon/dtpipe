using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using DtPipe.Adapters.SqlServer;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Tests.Unit.Conformance;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests.Integration.Conformance;

/// <summary>
/// The server half of the conformance table, whose gates are in the unit half: a server's cells need
/// the server. <c>tests/scripts/validate_type_conformance.sh</c> plays it with the endpoints of the test
/// infrastructure; without them each cell is skipped.
/// </summary>
[Collection("console-serial")]
public sealed class ServerConformanceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "dtpipe-server-face-" + Guid.NewGuid().ToString("N"));

    public ServerConformanceTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private const string Uuid = "6f9619ff-8b86-d011-b42d-00c04fc964ff";

    /// <summary>What a server's reader hands over on both paths, as its JSONL value; <c>named</c> is a refusal naming the column.</summary>
    private static readonly (string Server, string Expression, string Expected, string? KnownFalse)[] Reads =
    [
        ("pg", "true", "true", null),
        ("pg", "(-300)::smallint", "-300", null),
        ("pg", "(-5000000000)::bigint", "-5000000000", null),
        ("pg", "1.5::double precision", "1.5", null),
        ("pg", "4.2::numeric(10,2)", "4.20", null),
        ("pg", "4.29::numeric", "4.29", null),
        ("pg", "4.50::numeric", "4.5", null),
        ("pg", "1.5000000000000000000000::numeric", "1.5", null),
        ("pg", "0.0000000000000000000001::numeric", "named", null),
        ("pg", "'x'::text", "x", null),
        ("pg", "DATE '2024-01-02'", "2024-01-02", null),
        ("pg", "TIME '10:11:12.123456'", "10:11:12.123456", null),
        ("pg", "TIMESTAMP '2024-01-02 03:04:05'", "2024-01-02T03:04:05.0000000", null),
        ("pg", "TIMESTAMPTZ '2024-01-02 03:04:05+00'", "2024-01-02T03:04:05.0000000+00:00", null),
        ("pg", $"'{Uuid}'::uuid", Uuid, null),
        ("pg", "INTERVAL '1 hour'", "named", null),
        ("pg", "'infinity'::timestamp", "named", null),
        ("pg", "'-infinity'::timestamp", "named", null),
        ("pg", "'infinity'::timestamptz", "named", null),
        ("pg", "'-infinity'::timestamptz", "named", null),
        ("pg", "'infinity'::date", "named", null),
        ("pg", "'-infinity'::date", "named", null),
        ("pg", "ARRAY['infinity'::timestamp]", "named", null),
        ("pg", "ARRAY['-infinity'::date]", "named", null),
        ("pg", "DATE '0001-01-01'", "0001-01-01", null),
        ("pg", "TIMESTAMP '0001-01-01 00:00:00'", "0001-01-01T00:00:00.0000000", null),
        ("pg", "ARRAY[TIMESTAMP '2024-01-02 03:04:05', NULL]", """["2024-01-02T03:04:05.0000000",null]""", null),
        ("pg", "ARRAY[DATE '2024-01-02', NULL]", """["2024-01-02",null]""", null),
        ("pg", "DATE '0044-03-15 BC'", "named", null),
        ("pg", "'NaN'::numeric", "named", null),
        ("pg", "12345678901234567890123456789.5::numeric", "named", null),
        ("pg", "ARRAY[ARRAY['a','b'],ARRAY['c','d']]", "named", null),
        ("mysql", "CAST(-5000000000 AS SIGNED)", "-5000000000", null),
        ("mysql", "CAST(4.2 AS DECIMAL(10,2))", "4.20", null),
        ("mysql", "'x'", "x", null),
        ("mysql", "DATE '2024-01-02'", "2024-01-02", null),
        ("mysql", "CAST('10:11:12.123456' AS TIME(6))", "10:11:12.123456", null),
        ("mysql", "CAST('2024-01-02 03:04:05' AS DATETIME)", "2024-01-02T03:04:05.0000000", null),
        ("mssql", "CAST(1 AS BIT)", "true", null),
        ("mssql", "CAST(-70000 AS INT)", "-70000", null),
        ("mssql", "CAST(4.2 AS DECIMAL(10,2))", "4.20", null),
        ("mssql", "CAST(12.5 AS MONEY)", "12.5000", null),
        ("mssql", "'x'", "x", null),
        ("mssql", "CAST('2024-01-02' AS DATE)", "2024-01-02", null),
        ("mssql", "CAST('10:11:12.123456' AS TIME(6))", "10:11:12.123456", null),
        ("mssql", "CAST('2024-01-02T03:04:05' AS DATETIME2)", "2024-01-02T03:04:05.0000000", null),
        ("mssql", "CAST('2024-01-02T03:04:05+00:00' AS DATETIMEOFFSET)", "2024-01-02T03:04:05.0000000+00:00", null),
        ("mssql", "CAST('2024-01-02T03:04:05+02:00' AS DATETIMEOFFSET)", "2024-01-02T01:04:05.0000000+00:00", null),
        ("mssql", $"CAST('{Uuid}' AS UNIQUEIDENTIFIER)", Uuid, null),
        ("ora", "CAST(42 AS NUMBER(10))", "42", null),
        ("ora", "CAST(4.2 AS NUMBER(10,2))", "4.20", null),
        ("ora", "CAST(4.29 AS NUMBER(38,5))", "4.29000", null),
        ("ora", "'x'", "x", null),
        ("ora", "TIMESTAMP '2024-01-02 03:04:05'", "2024-01-02T03:04:05.0000000", null),
    ];

    /// <summary>What a server gives back of a column it was handed, as a JSONL value.</summary>
    private static readonly (string Server, string Case, string Expected, string? KnownFalse)[] Writes =
    [
        ("pg", "int64", "-5000000000", null),
        ("pg", "double", "1.5", null),
        ("pg", "decimal128(10,2)", "4.29", null),
        ("pg", "decimal-undeclared-fraction", "1.23456", null),
        ("pg", "date32", "2024-01-02", null),
        ("pg", "time64(us)", "10:11:12.123456", null),
        ("pg", "timestamp(us)", "2024-01-02T03:04:05.1234560", null),
        ("pg", "timestamp(us,UTC)", "2024-01-02T03:04:05.1234560+00:00", null),
        ("pg", "uuid", Uuid, null),
        ("pg", "struct", """{"a":1,"b":"x"}""", null),
        ("pg", "double-nan", "named", null),
        ("mysql", "int64", "-5000000000", null),
        ("mysql", "double", "1.5", null),
        ("mysql", "decimal128(10,2)", "4.29", null),
        ("mysql", "decimal-undeclared-fraction", "1.234560000000000000", null),
        ("mysql", "date32", "2024-01-02", null),
        ("mysql", "timestamp(us)", "2024-01-02T03:04:05.1234560", null),
        ("mssql", "int64", "-5000000000", null),
        ("mssql", "double", "1.5", null),
        ("mssql", "decimal128(10,2)", "4.29", null),
        ("mssql", "decimal-undeclared-fraction", "1.234560000000000000", null),
        ("mssql", "date32", "2024-01-02", null),
        ("mssql", "time64(us)", "10:11:12.123456", null),
        ("mssql", "timestamp(us)", "2024-01-02T03:04:05.1234560", null),
        ("mssql", "timestamp(us,UTC)", "2024-01-02T03:04:05.1234560+00:00", null),
        ("mssql", "uuid", Uuid, null),
        ("ora", "int64", "-5000000000", null),
        ("ora", "double", "1.5", null),
        ("ora", "decimal128(10,2)", "4.29", null),
        ("ora", "decimal-undeclared-fraction", "1.23456", "L4"),
        ("ora", "date32", "2024-01-02T00:00:00.0000000", null),
        ("ora", "timestamp(us)", "2024-01-02T03:04:05.1234560", null),
        ("ora", "timestamp(us,UTC)", "2024-01-02T03:04:05.1234560+00:00", null),
    ];

    /// <summary>
    /// What SQL Server gives back of a MAX column written on each road of its writer, into a table the
    /// writer creates or one already there. The engine reaches the row road only from a text reader with
    /// no step, which carries no bytes, so the cell drives the writer's two entries itself. What each
    /// road hands the bulk copy is the unit half, in <c>WriteConformanceTests</c>.
    /// </summary>
    private static readonly (string Case, bool Created, string Column, string Expected, string? KnownFalse)[] SqlServerMaxColumns =
    [
        ("binary", true, "VARBINARY(MAX)", "YWJj", null),
        ("binary", false, "VARBINARY(MAX)", "YWJj", null),
        ("string", true, "NVARCHAR(MAX)", "h\u00e9llo", null),
        ("int32", false, "NVARCHAR(MAX)", "-70000", null),
    ];

    private static readonly string[] Roads = ["row", "columnar"];

    public static TheoryData<string, bool, string> SqlServerMaxColumnCells()
    {
        var cells = new TheoryData<string, bool, string>();
        foreach (var (name, created, _, _, _) in SqlServerMaxColumns)
            foreach (var road in Roads)
                cells.Add(name, created, road);
        return cells;
    }

    public static TheoryData<string, string> ReadCells()
    {
        var cells = new TheoryData<string, string>();
        foreach (var (server, expression, _, _) in Reads) cells.Add(server, expression);
        return cells;
    }

    public static TheoryData<string, string> WriteCells()
    {
        var cells = new TheoryData<string, string>();
        foreach (var (server, name, _, _) in Writes) cells.Add(server, name);
        return cells;
    }

    [Theory]
    [MemberData(nameof(ReadCells))]
    public async Task A_Server_Reader_Reads_A_Cell_Alike_On_Both_Paths(string server, string expression)
    {
        var endpoint = Endpoint(server);
        var (_, _, expected, knownFalse) = Reads.Single(r => r.Server == server && r.Expression == expression);
        var query = server == "ora" ? $"SELECT {expression} AS \"v\" FROM dual" : $"SELECT {expression} AS v";

        var columnar = await ReadAsync(endpoint, query, row: false);
        var rows = await ReadAsync(endpoint, query, row: true);

        Judge(expected, knownFalse, columnar, rows);
    }

    private static readonly SemaphoreSlim TableLock = new(1, 1);

    [Theory]
    [MemberData(nameof(WriteCells))]
    public async Task A_Server_Writer_Gives_Back_What_It_Was_Handed(string server, string name)
    {
        await TableLock.WaitAsync();
        try
        {
            var endpoint = Endpoint(server);
            var (_, _, expected, knownFalse) = Writes.Single(w => w.Server == server && w.Case == name);
            var source = Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".arrow");
            ArrowTypeCases.Named(name).WriteIpc(source);
            var table = "tcc_" + name.Replace('(', '_').Replace(')', '_').Replace(',', '_').Replace('-', '_');
            string[] preExec = server == "pg" ? ["--pre-exec", $"DROP TABLE IF EXISTS {table} CASCADE"] : [];

            var written = await RunAsync(["-i", $"arrow:{source}", "-o", endpoint, "-t", table, "--strategy", "Recreate", .. preExec, "--no-stats"]);
            var actual = written.Exit == 0
                ? await ReadAsync(endpoint, server == "ora" ? $"SELECT v AS \"v\" FROM {table} WHERE v IS NOT NULL" : $"SELECT v FROM {table} WHERE v IS NOT NULL", row: false)
                : written.Named ? "named" : $"raw: exit {written.Exit}";

            Judge(expected, knownFalse, actual, actual);
        }
        finally
        {
            TableLock.Release();
        }
    }

    /// <summary>A decimal finer than the column already there is refused, where each engine would round it.</summary>
    [Theory]
    [InlineData("pg")]
    [InlineData("mysql")]
    [InlineData("mssql")]
    [InlineData("ora")]
    public async Task A_Server_Column_Does_Not_Round_A_Finer_Decimal(string server)
    {
        await TableLock.WaitAsync();
        try
        {
            var endpoint = Endpoint(server);
            const string table = "type_conformance_finer";
            var narrow = Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".arrow");
            var fine = Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".arrow");
            ArrowTypeCases.Named("decimal128(10,2)").WriteIpc(narrow);
            ArrowTypeCases.Named("decimal-undeclared-fraction").WriteIpc(fine);

            Assert.Equal(0, (await RunAsync(["-i", $"arrow:{narrow}", "-o", endpoint, "-t", table, "--strategy", "Recreate", "--no-stats"])).Exit);
            var appended = await RunAsync(["-i", $"arrow:{fine}", "-o", endpoint, "-t", table, "--strategy", "Append", "--no-stats"]);

            Assert.Equal(1, appended.Exit);
            Assert.True(appended.Named);
        }
        finally
        {
            TableLock.Release();
        }
    }

    [Theory]
    [MemberData(nameof(SqlServerMaxColumnCells))]
    public async Task A_Sql_Server_Max_Column_Gives_Back_What_Either_Road_Wrote(string name, bool created, string road)
    {
        await TableLock.WaitAsync();
        try
        {
            var endpoint = Endpoint("mssql");
            var (_, _, column, expected, knownFalse) = SqlServerMaxColumns.Single(c => c.Case == name && c.Created == created);
            var connection = ComponentSelector.Select(endpoint, "mssql").Cleaned;
            var table = $"tcc_max_{name}_{(created ? "created" : "existing")}_{road}";
            await using (var sql = new SqlConnection(connection))
            {
                await sql.OpenAsync();
                await using var command = sql.CreateCommand();
                command.CommandText = created ? $"DROP TABLE IF EXISTS {table}" : $"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} (v {column})";
                await command.ExecuteNonQueryAsync();
            }

            var failure = await WriteSqlServerAsync(connection, table, ArrowTypeCases.Named(name), created, rows: road == "row");
            var actual = failure ?? await ReadAsync(endpoint, $"SELECT v FROM {table} WHERE v IS NOT NULL", row: false);

            Judge(expected, knownFalse, actual, actual);
        }
        finally
        {
            TableLock.Release();
        }
    }

    // The writer as the engine drives it: initialised with the case's column, handed its values on one
    // road, completed. A failure reads `named` when it names the column, and `raw` with its type otherwise.
    private static async Task<string?> WriteSqlServerAsync(string connection, string table, ArrowTypeCase @case, bool created, bool rows)
    {
        var options = new SqlServerWriterOptions { Table = table, Strategy = created ? SqlServerWriteStrategy.Recreate : SqlServerWriteStrategy.Append };
        await using var writer = new SqlServerDataWriter(connection, options, NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance);
        var field = @case.Field;
        var array = @case.Build();
        try
        {
            await writer.InitializeAsync([new PipeColumnInfo(field.Name, ArrowTypeMap.GetClrTypeFromField(field), field.IsNullable)]);
            if (rows)
            {
                var values = Enumerable.Range(0, array.Length).Select(i => new[] { ArrowTypeMapper.GetValueForField(array, field, i) }).ToList();
                array.Dispose();
                await writer.WriteBatchAsync(values);
            }
            else
            {
                await writer.WriteRecordBatchAsync(new RecordBatch(new Schema([field], null), [array], array.Length));
            }
            await writer.CompleteAsync();
            return null;
        }
        catch (Exception ex)
        {
            return ex is ColumnValueRefusedException || ex.Message.Contains("'v'") ? "named" : $"raw: {ex.GetType().Name}";
        }
    }

    private static readonly string Rows = BatchContract.Rows.ToString();

    /// <summary>A server's reader keeps the batch contract of <c>ReadBatchesAsync</c> over a cursor that spans several batches.</summary>
    [Theory]
    [InlineData("pg")]
    [InlineData("mysql")]
    [InlineData("mssql")]
    [InlineData("ora")]
    public async Task A_Server_Reader_Yields_Distinct_Arrays_Of_At_Most_BatchSize_Rows(string server)
    {
        var endpoint = Endpoint(server);
        var connection = endpoint[(endpoint.IndexOf(':') + 1)..];
        const string digits = "(SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)";
        await using IStreamReader reader = server switch
        {
            "pg" => new DtPipe.Adapters.PostgreSQL.PostgreSqlReader(connection,
                $"SELECT i AS id, 'row ' || i AS v FROM generate_series(0, {Rows} - 1) AS t(i)"),
            "mysql" => new DtPipe.Adapters.MySql.MySqlReader(connection,
                $"SELECT n AS id, CONCAT('row ', n) AS v FROM (SELECT a.d + 10 * b.d + 100 * c.d + 1000 * e.d AS n FROM {digits} a, {digits} b, {digits} c, {digits} e) t WHERE n < {Rows} ORDER BY n"),
            "mssql" => new SqlServerReader(connection,
                $"SELECT n AS id, CONCAT('row ', n) AS v FROM (SELECT a.d + 10 * b.d + 100 * c.d + 1000 * e.d AS n FROM {digits} a, {digits} b, {digits} c, {digits} e) t WHERE n < {Rows} ORDER BY n",
                new SqlServerReaderOptions()),
            _ => new DtPipe.Adapters.Oracle.OracleReader(connection,
                $"SELECT LEVEL - 1 AS id, 'row ' || (LEVEL - 1) AS v FROM dual CONNECT BY LEVEL <= {Rows}",
                new DtPipe.Adapters.Oracle.OracleReaderOptions()),
        };

        await BatchContract.AssertAsync(reader);
    }

    private static string Endpoint(string server)
    {
        var endpoint = Environment.GetEnvironmentVariable("DTPIPE_CONFORMANCE_" + server.ToUpperInvariant());
        if (string.IsNullOrEmpty(endpoint)) Assert.Skip("Played by validate_type_conformance.sh.");
        return endpoint;
    }

    private async Task<string> ReadAsync(string endpoint, string query, bool row)
    {
        var target = Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".jsonl");
        string[] path = row ? ["--compute", "zz:1"] : [];
        var run = await RunAsync(["-i", endpoint, "-q", query, .. path, "-o", $"jsonl:{target}", "--no-stats"]);
        if (run.Exit != 0) return run.Named ? "named" : $"raw: exit {run.Exit}";

        using var line = JsonDocument.Parse(File.ReadLines(target).First());
        var value = line.RootElement.GetProperty("v");
        return value.ValueKind == JsonValueKind.String ? value.GetString()! : value.GetRawText();
    }

    private static async Task<(int Exit, bool Named)> RunAsync(string[] args)
    {
        var (exit, captured) = await ConsoleSerialLock.CaptureAsync(() => Program.Main(args));
        return (exit, captured.Contains("'v'"));
    }

    private static void Judge(string expected, string? knownFalse, string columnar, string rows)
    {
        if (knownFalse is { } lot)
        {
            Assert.False(columnar == expected && rows == expected, $"This cell now reads '{rows}': the correction of {lot} is in, drop its mark.");
            return;
        }

        Assert.Equal(expected, columnar);
        Assert.Equal(expected, rows);
    }
}
