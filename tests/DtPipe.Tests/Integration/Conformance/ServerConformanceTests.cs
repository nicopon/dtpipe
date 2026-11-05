using System.Text.Json;
using DtPipe.Tests.Unit.Conformance;
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
        ("pg", "4.29::numeric", "4.29", "L4"),
        ("pg", "'x'::text", "x", null),
        ("pg", "DATE '2024-01-02'", "2024-01-02", null),
        ("pg", "TIME '10:11:12.123456'", "10:11:12.123456", null),
        ("pg", "TIMESTAMP '2024-01-02 03:04:05'", "2024-01-02T03:04:05.0000000", null),
        ("pg", "TIMESTAMPTZ '2024-01-02 03:04:05+00'", "2024-01-02T03:04:05.0000000+00:00", null),
        ("pg", $"'{Uuid}'::uuid", Uuid, null),
        ("pg", "INTERVAL '1 hour'", "named", null),
        ("pg", "'infinity'::timestamp", "named", "L2"),
        ("pg", "'-infinity'::timestamp", "named", "L2"),
        ("pg", "'NaN'::numeric", "named", "L2"),
        ("mysql", "CAST(-5000000000 AS SIGNED)", "-5000000000", null),
        ("mysql", "CAST(4.2 AS DECIMAL(10,2))", "4.20", null),
        ("mysql", "'x'", "x", null),
        ("mysql", "DATE '2024-01-02'", "2024-01-02", null),
        ("mysql", "CAST('10:11:12.123456' AS TIME(6))", "10:11:12.123456", null),
        ("mysql", "CAST('2024-01-02 03:04:05' AS DATETIME)", "2024-01-02T03:04:05.0000000", null),
        ("mssql", "CAST(1 AS BIT)", "true", null),
        ("mssql", "CAST(-70000 AS INT)", "-70000", null),
        ("mssql", "CAST(4.2 AS DECIMAL(10,2))", "4.20", null),
        ("mssql", "CAST(12.5 AS MONEY)", "12.5000", "L4"),
        ("mssql", "'x'", "x", null),
        ("mssql", "CAST('2024-01-02' AS DATE)", "2024-01-02", null),
        ("mssql", "CAST('10:11:12.123456' AS TIME(6))", "10:11:12.123456", null),
        ("mssql", "CAST('2024-01-02T03:04:05' AS DATETIME2)", "2024-01-02T03:04:05.0000000", null),
        ("mssql", "CAST('2024-01-02T03:04:05+00:00' AS DATETIMEOFFSET)", "2024-01-02T03:04:05.0000000+00:00", null),
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
        ("pg", "double-nan", "named", "L2"),
        ("mysql", "int64", "-5000000000", null),
        ("mysql", "double", "1.5", null),
        ("mysql", "decimal128(10,2)", "4.29", null),
        ("mysql", "decimal-undeclared-fraction", "1.23456", "L3"),
        ("mysql", "date32", "2024-01-02", null),
        ("mysql", "timestamp(us)", "2024-01-02T03:04:05.1234560", null),
        ("mssql", "int64", "-5000000000", null),
        ("mssql", "double", "1.5", null),
        ("mssql", "decimal128(10,2)", "4.29", null),
        ("mssql", "decimal-undeclared-fraction", "1.23456", "L3"),
        ("mssql", "date32", "2024-01-02", null),
        ("mssql", "time64(us)", "10:11:12.123456", null),
        ("mssql", "timestamp(us)", "2024-01-02T03:04:05.1234560", null),
        ("mssql", "timestamp(us,UTC)", "2024-01-02T03:04:05.1234560+00:00", null),
        ("mssql", "uuid", Uuid, null),
        ("ora", "int64", "-5000000000", null),
        ("ora", "double", "1.5", null),
        ("ora", "decimal128(10,2)", "4.29", null),
        ("ora", "decimal-undeclared-fraction", "1.23456", "L3"),
        ("ora", "date32", "2024-01-02T00:00:00.0000000", null),
        ("ora", "timestamp(us)", "2024-01-02T03:04:05.1234560", null),
        ("ora", "timestamp(us,UTC)", "2024-01-02T03:04:05.1234560+00:00", null),
    ];

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

    [Theory]
    [MemberData(nameof(WriteCells))]
    public async Task A_Server_Writer_Gives_Back_What_It_Was_Handed(string server, string name)
    {
        var endpoint = Endpoint(server);
        var (_, _, expected, knownFalse) = Writes.Single(w => w.Server == server && w.Case == name);
        var source = Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".arrow");
        ArrowTypeCases.Named(name).WriteIpc(source);
        const string table = "type_conformance_cell";

        var written = await RunAsync(["-i", $"arrow:{source}", "-o", endpoint, "-t", table, "--strategy", "Recreate", "--no-stats"]);
        var actual = written.Exit == 0
            ? await ReadAsync(endpoint, server == "ora" ? $"SELECT \"v\" FROM {table} WHERE \"v\" IS NOT NULL" : $"SELECT v FROM {table} WHERE v IS NOT NULL", row: false)
            : written.Named ? "named" : $"raw: exit {written.Exit}";

        Judge(expected, knownFalse, actual, actual);
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
        var (stdout, stderr) = (Console.Out, Console.Error);
        var captured = new StringWriter();
        Console.SetOut(captured);
        Console.SetError(captured);
        try
        {
            var exit = await Program.Main(args);
            return (exit, captured.ToString().Contains("'v'"));
        }
        finally { Console.SetOut(stdout); Console.SetError(stderr); }
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
