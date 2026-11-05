using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Adapters.MySql;
using DtPipe.Adapters.Oracle;
using DtPipe.Adapters.PostgreSQL;
using DtPipe.Adapters.SqlServer;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests.Unit.Conformance;

/// <summary>
/// The write face of the conformance table: writer x Arrow type. A cell writes the column so that a
/// third party reads back the value under the type the format has for it, or the gate refuses it
/// before the target exists; it never turns a column into something else and exits 0.
/// A cell marked with a lot is known false and is corrected by that lot: it must still be false, so a
/// correction cannot land without entering the gate.
/// </summary>
[Collection("console-serial")]
public sealed class WriteConformanceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "dtpipe-write-face-" + Guid.NewGuid().ToString("N"));

    public WriteConformanceTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private static readonly string[] Writers = ["csv", "jsonl", "parquet", "arrow", "duck", "sqlite", "checksum"];
    private static readonly string[] ServerWriters = ["pg", "mysql", "mssql", "ora"];

    private readonly record struct Cell(string Expected, string? KnownFalse = null)
    {
        public static implicit operator Cell(string expected) => new(expected);
    }

    private static Cell L3(string expected) => new(expected, "L3");
    private static Cell L4(string expected) => new(expected, "L4");

    private sealed record Row(string Case, Cell Csv, Cell Jsonl, Cell Parquet, Cell Arrow, Cell Duck, Cell Sqlite, Cell Checksum)
    {
        public Cell For(string writer) => writer switch
        {
            "csv" => Csv,
            "jsonl" => Jsonl,
            "parquet" => Parquet,
            "arrow" => Arrow,
            "duck" => Duck,
            "sqlite" => Sqlite,
            _ => Checksum
        };
    }

    // case, then csv, jsonl, parquet, arrow, duck, sqlite, checksum
    private static readonly Row[] Table =
    [
        new("null", "", "null", L3("refused"), "null | null", "VARCHAR | null", "null | null", "ok"),
        new("bool", "true", "true", "BOOLEAN | true", "bool | Boolean:true", "BOOLEAN | true", "integer | 1", "ok"),
        new("bool8", "true", "true", "BOOLEAN | true", "int8 | Boolean:true", "BOOLEAN | true", "integer | 1", "ok"),
        new("int8", "-5", "-5", "TINYINT | -5", "int8 | SByte:-5", "TINYINT | -5", "integer | -5", "ok"),
        new("int16", "-300", "-300", "SMALLINT | -300", "int16 | Int16:-300", "SMALLINT | -300", "integer | -300", "ok"),
        new("int32", "-70000", "-70000", "INTEGER | -70000", "int32 | Int32:-70000", "INTEGER | -70000", "integer | -70000", "ok"),
        new("int64", "-5000000000", "-5000000000", "BIGINT | -5000000000", "int64 | Int64:-5000000000", "BIGINT | -5000000000", "integer | -5000000000", "ok"),
        new("uint8", "200", "200", "UTINYINT | 200", "uint8 | Byte:200", "UTINYINT | 200", "integer | 200", "ok"),
        new("uint16", "60000", "60000", "USMALLINT | 60000", "uint16 | UInt16:60000", "USMALLINT | 60000", "integer | 60000", "ok"),
        new("uint32", "4000000000", "4000000000", "UINTEGER | 4000000000", "uint32 | UInt32:4000000000", "UINTEGER | 4000000000", "integer | 4000000000", "ok"),
        new("uint64", "18000000000000000000", "18000000000000000000", "UBIGINT | 18000000000000000000", "uint64 | UInt64:18000000000000000000", "UBIGINT | 18000000000000000000", "refused: utf8", "ok"),
        new("int128", "170141183460469231731687303715884105727", "170141183460469231731687303715884105727", "refused: utf8", "fixed_size_binary | Int128:170141183460469231731687303715884105727", "HUGEINT | 170141183460469231731687303715884105727", "refused: utf8", "ok"),
        new("int128-min", "-170141183460469231731687303715884105728", "-170141183460469231731687303715884105728", "refused: utf8", "fixed_size_binary | Int128:-170141183460469231731687303715884105728", "HUGEINT | -170141183460469231731687303715884105728", "refused: utf8", "ok"),
        new("uint128", "340282366920938463463374607431768211455", "340282366920938463463374607431768211455", "refused: utf8", "fixed_size_binary | UInt128:340282366920938463463374607431768211455", "refused: utf8", "refused: utf8", "ok"),
        new("halffloat", L3("refused"), L3("refused"), L3("refused"), "halffloat | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("float", "1.5", "1.5", "FLOAT | 1.5", "float | Single:1.5", "FLOAT | 1.5", "real | 1.5", "ok"),
        new("double", "1.5", "1.5", "DOUBLE | 1.5", "double | Double:1.5", "DOUBLE | 1.5", "real | 1.5", "ok"),
        new("double-nan", "NaN", "named", "DOUBLE | nan", "double | Double:NaN", "DOUBLE | nan", "named", "ok"),
        new("double-inf", "Infinity", "named", "DOUBLE | inf", "double | Double:Infinity", "DOUBLE | inf", "real | Inf", "ok"),
        new("decimal32", "4.29", "4.29", L4("DECIMAL(9,2) | 4.29"), "decimal32 | Decimal:4.29", L4("DECIMAL(9,2) | 4.29"), "real | 4.29", "ok"),
        new("decimal64", "4.29", "4.29", L4("DECIMAL(18,2) | 4.29"), "decimal64 | Decimal:4.29", L4("DECIMAL(18,2) | 4.29"), "real | 4.29", "ok"),
        new("decimal128(10,2)", "4.29", "4.29", "DECIMAL(10,2) | 4.29", "decimal128 | Decimal:4.29", "DECIMAL(10,2) | 4.29", "real | 4.29", "ok"),
        new("decimal128(38,18)", "4.290000000000000000", "4.290000000000000000", "DECIMAL(38,18) | 4.290000000000000000", "decimal128 | Decimal:4.290000000000000000", "DECIMAL(38,18) | 4.290000000000000000", "real | 4.29", "ok"),
        new("decimal-undeclared", L4("4.29"), L4("4.29"), "DECIMAL(38,18) | 4.290000000000000000", "decimal128 | Decimal:4.290000000000000000", L3("DECIMAL(38,18) | 4.290000000000000000"), "real | 4.29", "ok"),
        new("decimal128-wide", "12345678901234567.891000000000", "12345678901234567.891000000000", "DECIMAL(38,18) | 12345678901234567.891000000000000000", "decimal128 | Decimal:12345678901234567.891000000000", "DECIMAL(38,18) | 12345678901234567.891000000000000000", L3("text | 12345678901234567.891000000000"), "ok"),
        new("decimal-undeclared-wide", L4("12345678901234567.891"), L4("12345678901234567.891"), "DECIMAL(38,18) | 12345678901234567.891000000000000000", "decimal128 | Decimal:12345678901234567.891000000000", L3("DECIMAL(38,18) | 12345678901234567.891000000000000000"), L3("text | 12345678901234567.891"), "ok"),
        new("decimal-undeclared-fraction", L4("1.23456"), L4("1.23456"), "DECIMAL(38,18) | 1.234560000000000000", "decimal128 | Decimal:1.234560000000000000", L3("DECIMAL(38,18) | 1.234560000000000000"), "real | 1.23456", "ok"),
        new("decimal256", "4.2900000000", "4.2900000000", "DECIMAL(38,18) | 4.290000000000000000", "decimal256 | Decimal:4.2900000000", L4("DECIMAL(38,10) | 4.2900000000"), "real | 4.29", "ok"),
        new("string", "héllo", @"""h\u00E9llo""", "VARCHAR | héllo", "utf8 | String:héllo", "VARCHAR | héllo", "text | héllo", "ok"),
        new("largestring", L3("refused"), L3("refused"), L3("refused"), "large_utf8 | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("stringview", L3("refused"), L3("refused"), L3("refused"), "utf8view | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("binary", "YWJj", @"""YWJj""", "BLOB | abc", "binary | Bytes:616263", "BLOB | abc", "blob | 616263", "ok"),
        new("largebinary", L3("refused"), L3("refused"), L3("refused"), "large_binary | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("binaryview", L3("refused"), L3("refused"), L3("refused"), "binaryview | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("fixedbinary(8)", "YWJjZGVmZ2g=", @"""YWJjZGVmZ2g=""", "BLOB | abcdefgh", "fixed_size_binary | Bytes:6162636465666768", "BLOB | abcdefgh", "blob | 6162636465666768", "ok"),
        new("uuid", "6f9619ff-8b86-d011-b42d-00c04fc964ff", @"""6f9619ff-8b86-d011-b42d-00c04fc964ff""", "UUID | 6f9619ff-8b86-d011-b42d-00c04fc964ff", "fixed_size_binary | Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff", "UUID | 6f9619ff-8b86-d011-b42d-00c04fc964ff", "text | 6F9619FF-8B86-D011-B42D-00C04FC964FF", "ok"),
        new("date32", "2024-01-02", @"""2024-01-02""", "DATE | 2024-01-02", "date32 | DateOnly:2024-01-02", "DATE | 2024-01-02", "text | 2024-01-02", "ok"),
        new("date64", "2024-01-02 00:00:00.000000", @"""2024-01-02T00:00:00.0000000""", "TIMESTAMP | 2024-01-02 00:00:00", "date64 | DateTime:2024-01-02T00:00:00.0000000", "TIMESTAMP | 2024-01-02 00:00:00", "text | 2024-01-02 00:00:00", "ok"),
        new("time32(ms)", "10:11:12.123000", @"""10:11:12.123000""", "TIME | 10:11:12.123", "time32 | TimeOnly:10:11:12.1230000", "TIME | 10:11:12.123", "text | 10:11:12.123000", "ok"),
        new("time64(us)", "10:11:12.123456", @"""10:11:12.123456""", "TIME | 10:11:12.123456", "time64 | TimeOnly:10:11:12.1234560", "TIME | 10:11:12.123456", "text | 10:11:12.123456", "ok"),
        new("timestamp(s)", "2024-01-02 03:04:05.000000", @"""2024-01-02T03:04:05.0000000""", "TIMESTAMP | 2024-01-02 03:04:05", "timestamp | DateTime:2024-01-02T03:04:05.0000000", "TIMESTAMP | 2024-01-02 03:04:05", "text | 2024-01-02 03:04:05", "ok"),
        new("timestamp(us)", "2024-01-02 03:04:05.123456", @"""2024-01-02T03:04:05.1234560""", "TIMESTAMP | 2024-01-02 03:04:05.123456", "timestamp | DateTime:2024-01-02T03:04:05.1234560", "TIMESTAMP | 2024-01-02 03:04:05.123456", "text | 2024-01-02 03:04:05.123456", "ok"),
        new("timestamp(ns)", "2024-01-02 03:04:05.123456", @"""2024-01-02T03:04:05.1234560""", "TIMESTAMP | 2024-01-02 03:04:05.123456", "timestamp | DateTime:2024-01-02T03:04:05.1234560", "TIMESTAMP | 2024-01-02 03:04:05.123456", "text | 2024-01-02 03:04:05.123456", "ok"),
        new("timestamp(us,UTC)", "2024-01-02 03:04:05.123456+00:00", @"""2024-01-02T03:04:05.1234560\u002B00:00""", "TIMESTAMP WITH TIME ZONE | 2024-01-02 03:04:05.123456+00", "timestamp | DateTimeOffset:2024-01-02T03:04:05.1234560+00:00", "TIMESTAMP WITH TIME ZONE | 2024-01-02 03:04:05.123456+00", "text | 2024-01-02 03:04:05.123456+00:00", "ok"),
        new("timestamp(us,Asia/Tokyo)", "2024-01-02 03:04:05.123456+00:00", @"""2024-01-02T03:04:05.1234560\u002B00:00""", "TIMESTAMP WITH TIME ZONE | 2024-01-02 03:04:05.123456+00", "timestamp | DateTimeOffset:2024-01-02T03:04:05.1234560+00:00", "TIMESTAMP WITH TIME ZONE | 2024-01-02 03:04:05.123456+00", "text | 2024-01-02 03:04:05.123456+00:00", "ok"),
        new("duration(us)", "00:01:30", @"""00:01:30""", "refused: int64, utf8", "duration | TimeSpan:00:01:30", L3("refused"), "text | 00:01:30", "ok"),
        new("interval(ym)", L3("refused"), L3("refused"), L3("refused"), "interval | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("interval(dt)", L3("refused"), L3("refused"), L3("refused"), "interval | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("interval(mdn)", L3("refused"), L3("refused"), L3("refused"), "interval | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("list<int32>", @"""[1,2]""", "[1,2]", "INTEGER[] | [1, 2]", "list | [Int32:1,Int32:2]", L3("INTEGER[] | [1, 2]"), "text | [1,2]", "ok"),
        new("list<decimal(10,2)>", "[4.29]", "[4.29]", L4("DECIMAL(10,2)[] | [4.29]"), "list | [Decimal:4.29]", L3("DECIMAL(10,2)[] | [4.29]"), "text | [4.29]", "ok"),
        new("list<timestamp>", @"""[""""2024-01-02T03:04:05.123456""""]""", """["2024-01-02T03:04:05.1234560"]""", L3("refused"), "list | [DateTime:2024-01-02T03:04:05.1234560]", L3("TIMESTAMP[] | [2024-01-02 03:04:05.123456]"), """text | ["2024-01-02T03:04:05.123456"]""", "ok"),
        new("list<uuid>", @"""[""""6f9619ff-8b86-d011-b42d-00c04fc964ff""""]""", """["6f9619ff-8b86-d011-b42d-00c04fc964ff"]""", L3("refused"), "list | [Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff]", L3("UUID[] | [6f9619ff-8b86-d011-b42d-00c04fc964ff]"), """text | ["6f9619ff-8b86-d011-b42d-00c04fc964ff"]""", "ok"),
        new("list<struct>", @"""[{""""a"""":1}]""", """[{"a":1}]""", L3("refused"), L3("list | [{a=Int32:1}]"), L3("refused"), L3("""text | [{"a":1}]"""), "ok"),
        new("largelist", L3("refused"), L3("refused"), L3("refused"), "large_list | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("fixedlist", @"""[1,2]""", "[1,2]", L3("refused"), "fixed_size_list | [Int32:1,Int32:2]", L3("INTEGER[2] | [1, 2]"), "text | [1,2]", "ok"),
        new("listview", L3("refused"), L3("refused"), L3("refused"), "listview | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("largelistview", L3("refused"), L3("refused"), L3("refused"), "large_listview | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("struct", @"""{""""a"""":1,""""b"""":""""x""""}""", """{"a":1,"b":"x"}""", L3("refused"), L3("struct | {a=Int32:1,b=String:x}"), L3("refused"), L3("""text | {"a":1,"b":"x"}"""), "ok"),
        new("map", @"""{""""a"""":1}""", """{"a":1}""", L3("refused"), L3("map | {a=Int32:1}"), L3("refused"), L3("""text | {"a":1}"""), "ok"),
        new("union(sparse)", L3("refused"), L3("refused"), L3("refused"), "union | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("union(dense)", L3("refused"), L3("refused"), L3("refused"), "union | 2 rows", L3("refused"), L3("refused"), L3("refused")),
        new("dictionary", "a", @"""a""", L3("VARCHAR | a"), L3("utf8 | String:a"), "VARCHAR | a", "text | a", "ok"),
        new("runend", L3("refused"), L3("refused"), L3("refused"), "run_end_encoded | 2 rows", L3("refused"), L3("refused"), L3("refused")),
    ];

    private sealed record GateRow(string Case, Cell Pg, Cell MySql, Cell SqlServer, Cell Oracle)
    {
        public Cell For(string writer) => writer switch
        {
            "pg" => Pg,
            "mysql" => MySql,
            "mssql" => SqlServer,
            _ => Oracle
        };
    }

    // The server writers' gates, which need no server. A case not listed is accepted by all four.
    private static readonly GateRow[] Gates =
    [
        new("int8", "refused: int16", "accepted", "refused: int16", "accepted"),
        new("uint8", "refused: int16", "accepted", "accepted", "accepted"),
        new("uint16", "refused: int32", "accepted", "refused: int32", "accepted"),
        new("uint32", "refused: int64", "accepted", "refused: int64", "accepted"),
        new("uint64", "refused: decimal128", "accepted", "refused: decimal128", "accepted"),
        new("int128", "refused: utf8", "refused: utf8", "refused: utf8", "refused: utf8"),
        new("int128-min", "refused: utf8", "refused: utf8", "refused: utf8", "refused: utf8"),
        new("uint128", "refused: utf8", "refused: utf8", "refused: utf8", "refused: utf8"),
        new("halffloat", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("largestring", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("stringview", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("largebinary", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("binaryview", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("time32(ms)", "accepted", "accepted", "accepted", "refused: utf8"),
        new("time64(us)", "accepted", "accepted", "accepted", "refused: utf8"),
        new("timestamp(us,UTC)", "accepted", "refused: timestamp", "accepted", "accepted"),
        new("timestamp(us,Asia/Tokyo)", "accepted", "refused: timestamp", "accepted", "accepted"),
        new("interval(ym)", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("interval(dt)", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("interval(mdn)", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("largelist", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("listview", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("largelistview", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("union(sparse)", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("union(dense)", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
        new("runend", L3("refused"), L3("refused"), L3("refused"), L3("refused")),
    ];

    public static TheoryData<string, string> Cells()
    {
        var cells = new TheoryData<string, string>();
        foreach (var row in Table)
            foreach (var writer in Writers)
                cells.Add(row.Case, writer);
        return cells;
    }

    public static TheoryData<string, string> GateCells()
    {
        var cells = new TheoryData<string, string>();
        foreach (var @case in ArrowTypeCases.All)
            foreach (var writer in ServerWriters)
                cells.Add(@case.Name, writer);
        return cells;
    }

    [Theory]
    [MemberData(nameof(Cells))]
    public async Task A_Writer_Writes_The_Column_Or_Refuses_It(string name, string writer)
    {
        var source = Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".arrow");
        ArrowTypeCases.Named(name).WriteIpc(source);

        var actual = await WriteOutcome.PlayAsync(writer, source, _dir);

        Judge(Table.Single(r => r.Case == name).For(writer), actual);
    }

    [Theory]
    [MemberData(nameof(GateCells))]
    public void A_Server_Writer_Answers_For_Every_Type(string name, string writer)
    {
        var row = Gates.SingleOrDefault(r => r.Case == name);
        var cell = row?.For(writer) ?? "accepted";

        Judge(cell, WriteOutcome.GateVerdict(writer, ArrowTypeCases.Named(name).Field));
    }

    /// <summary>A text reader carries a value that fits a decimal and not the column's Arrow width.</summary>
    [Theory]
    [InlineData("parquet")]
    [InlineData("arrow")]
    public async Task A_Value_The_Column_Cannot_Take_Is_Named_On_The_Way_To_A_Columnar_Writer(string writer)
    {
        var source = Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".csv");
        File.WriteAllText(source, "id,v\n1,4.29\n2,0.0000000000000000000001\n");
        var target = Path.Combine(_dir, Guid.NewGuid().ToString("N") + "." + writer);

        var (stdout, stderr) = (Console.Out, Console.Error);
        var captured = new StringWriter();
        Console.SetOut(captured);
        Console.SetError(captured);
        int exit;
        try { exit = await Program.Main(["-i", source, "--column-types", "id:int32,v:decimal", "-o", $"{writer}:{target}", "--no-stats"]); }
        finally { Console.SetOut(stdout); Console.SetError(stderr); }

        var text = Regex.Replace(captured.ToString(), @"\s+", " ");
        Assert.Equal(1, exit);
        Assert.Contains("Record 2: column 'v'", text);
        Assert.Contains("declare the column 'string'", text);
        Assert.DoesNotContain("0.0000000000000000000001", text);
    }

    [Fact]
    public async Task A_128_Bit_Integer_Keeps_Its_Type_Through_A_Row_Transformer()
    {
        var target = Path.Combine(_dir, "out.duckdb");

        Assert.Equal(0, await Program.Main(["-i", "duck::memory:", "-q", "select 170141183460469231731687303715884105727::HUGEINT as v, 1 as id",
            "--compute", "id2:row.id", "-o", $"duck:{target}", "-t", "t", "--no-stats"]));

        Assert.Equal("HUGEINT | 170141183460469231731687303715884105727", WriteOutcome.ReadBack("duck", target));
    }

    [Fact]
    public void Every_Arrow_Type_Has_A_Case()
    {
        var covered = ArrowTypeCases.All.Select(c => c.TypeId).ToHashSet();

        var missing = Enum.GetValues<ArrowTypeId>()
            .Where(id => !covered.Contains(id) && !ArrowTypeCases.NoCase.ContainsKey(id));

        Assert.Empty(missing);
    }

    [Fact]
    public void Every_Case_Has_A_Row() => Assert.Equal(
        ArrowTypeCases.All.Select(c => c.Name).Order(StringComparer.Ordinal),
        Table.Select(r => r.Case).Order(StringComparer.Ordinal));

    private static void Judge(Cell cell, string actual)
    {
        var right = cell.Expected == "refused" ? actual.StartsWith("refused", StringComparison.Ordinal) : actual == cell.Expected;

        if (cell.KnownFalse is { } lot)
            Assert.False(right, $"This cell now reads '{actual}': the correction of {lot} is in, drop its mark.");
        else
            Assert.Equal(cell.Expected, actual);
    }
}

internal static class WriteOutcome
{
    /// <summary>
    /// What a server writer's gate answers for a field: it reads the schema alone, so the cell needs no
    /// server. Whether the server then holds the value is the validator's half.
    /// </summary>
    public static string GateVerdict(string writer, Field field)
    {
        const string connection = "Host=none;Database=none;Server=none";
        IColumnTypeGate gate = writer switch
        {
            "pg" => new PostgreSqlDataWriter(connection, new PostgreSqlWriterOptions(), NullLogger<PostgreSqlDataWriter>.Instance, PostgreSqlTypeConverter.Instance),
            "mysql" => new MySqlDataWriter(connection, new MySqlWriterOptions(), NullLogger<MySqlDataWriter>.Instance, MySqlTypeConverter.Instance),
            "mssql" => new SqlServerDataWriter(connection, new SqlServerWriterOptions(), NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance),
            "ora" => new OracleDataWriter("Data Source=none", new OracleWriterOptions(), NullLogger<OracleDataWriter>.Instance, OracleTypeConverter.Instance),
            _ => throw new ArgumentOutOfRangeException(nameof(writer), writer, null)
        };

        var refusals = gate.RefuseColumns(new Schema([field], null));
        return refusals.Count == 0 ? "accepted" : "refused: " + string.Join(", ", refusals[0].Accepted.Select(a => a.Name));
    }

    /// <summary>
    /// One cell played through the real engine: the case's IPC file into the writer, then the target read
    /// back by a third party. A failure reads <c>refused</c> when the gate answered before the target
    /// existed, <c>named</c> when the error names the column, and <c>raw</c> otherwise.
    /// </summary>
    public static async Task<string> PlayAsync(string writer, string source, string dir)
    {
        var name = Guid.NewGuid().ToString("N");
        var target = Path.Combine(dir, name + (writer == "duck" ? ".duckdb" : "." + writer));
        string[] output = writer is "duck" or "sqlite" ? ["-o", $"{writer}:{target}", "-t", "t"] : ["-o", $"{writer}:{target}"];

        var (stdout, stderr) = (Console.Out, Console.Error);
        var captured = new StringWriter();
        Console.SetOut(captured);
        Console.SetError(captured);
        int exit;
        try { exit = await Program.Main(["-i", $"arrow:{source}", .. output, "--no-stats"]); }
        finally { Console.SetOut(stdout); Console.SetError(stderr); }

        if (exit == 0) return ReadBack(writer, target);

        var text = Regex.Replace(captured.ToString(), @"\s+", " ");
        if (text.Contains("ColumnTypeRefusedException") && text.Contains("'v'") && !File.Exists(target))
        {
            var accepted = Regex.Match(text, @"accepted instead: (.+?)\.");
            return accepted.Success ? $"refused: {accepted.Groups[1].Value}" : "refused";
        }
        if (text.Contains("'v'")) return "named";
        var exception = Regex.Match(text, @"-> (\w+Exception)");
        return $"raw: {(exception.Success ? exception.Groups[1].Value : "exit " + exit)}";
    }

    internal static string ReadBack(string writer, string target)
    {
        switch (writer)
        {
            case "csv":
                return File.ReadAllLines(target)[1];
            case "jsonl":
                var line = File.ReadAllLines(target)[0];
                return line[(line.IndexOf(':') + 1)..^1];
            case "checksum":
                return "ok";
            case "arrow":
                return ReadBackArrow(target);
            case "sqlite":
                return ReadBackSqlite(target);
            default:
                return ReadBackDuck(writer, target);
        }
    }

    private static string ReadBackArrow(string target)
    {
        using var stream = File.OpenRead(target);
        using var reader = new ArrowFileReader(stream);
        using var batch = reader.ReadNextRecordBatch();
        var field = batch.Schema.GetFieldByIndex(0);
        try
        {
            return $"{field.DataType.Name} | {ConformanceValue.Of(ArrowTypeMapper.GetValueForField(batch.Column(0), field, 0))}";
        }
        catch (NotSupportedException)
        {
            // dtpipe has no reading of this type; the file carries it under its own name.
            return $"{field.DataType.Name} | {batch.Column(0).Length} rows";
        }
    }

    private static string ReadBackSqlite(string target)
    {
        using var connection = new SqliteConnection($"Data Source={target};Pooling=False");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT typeof(v) || ' | ' || COALESCE(CASE typeof(v) WHEN 'blob' THEN hex(v) ELSE v END, 'null') FROM t ORDER BY rowid LIMIT 1";
        return (string)command.ExecuteScalar()!;
    }

    private static string ReadBackDuck(string writer, string target)
    {
        using var duck = new DuckDBConnection("DataSource=:memory:");
        duck.Open();
        using var query = duck.CreateCommand();
        var from = $"read_parquet('{target}')";
        if (writer == "duck")
        {
            query.CommandText = $"ATTACH '{target}' AS f (READ_ONLY)";
            query.ExecuteNonQuery();
            from = "f.t";
        }
        query.CommandText = $"SET TimeZone = 'UTC'; SELECT typeof(v) || ' | ' || COALESCE(v::VARCHAR, 'null') FROM {from} LIMIT 1";
        // The sign of a NaN is the platform's printf.
        return ((string)query.ExecuteScalar()!).Replace("-nan", "nan");
    }
}
