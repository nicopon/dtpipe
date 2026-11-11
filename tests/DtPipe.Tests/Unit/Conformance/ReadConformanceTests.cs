using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Adapters.Arrow;
using DtPipe.Adapters.Common;
using DtPipe.Adapters.Csv;
using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.Generate;
using DtPipe.Adapters.JsonL;
using DtPipe.Adapters.Parquet;
using DtPipe.Adapters.Sqlite;
using DtPipe.Adapters.Xml;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DtPipe.Tests.Unit.Conformance;

/// <summary>How a reader reads one cell, on each of its paths.</summary>
internal static class ReadOutcome
{
    /// <summary>
    /// The value of column <c>v</c> in the first row on the columnar path, null when the reader has none.
    /// A failure reads <c>named</c> when it names the column and <c>raw</c> otherwise.
    /// </summary>
    public static async Task<string?> ColumnarAsync(Func<IStreamReader> open)
    {
        await using var reader = open();
        if (reader is not IColumnarStreamReader columnar) return null;
        try
        {
            await reader.OpenAsync();
            var values = new List<object?>();
            await foreach (var batch in columnar.ReadRecordBatchesAsync())
            {
                using (batch)
                {
                    var index = batch.Schema.GetFieldIndex("v");
                    var field = batch.Schema.GetFieldByIndex(index);
                    for (var row = 0; row < batch.Length; row++)
                        values.Add(ArrowTypeMapper.GetValueForField(batch.Column(index), field, row));
                }
            }
            return Spell(values);
        }
        catch (Exception e) when (e is not Xunit.Sdk.IAssertionException)
        {
            return Failure(e);
        }
    }

    /// <summary>The same cell on the row path.</summary>
    public static async Task<string> RowsAsync(Func<IStreamReader> open)
    {
        await using var reader = open();
        try
        {
            await reader.OpenAsync();
            var index = reader.Columns!.Select((c, i) => (c.Name, i)).First(c => c.Name == "v").i;
            var values = new List<object?>();
            await foreach (var batch in reader.ReadBatchesAsync(16))
            {
                foreach (var row in batch.ToArray()) values.Add(row[index]);
            }
            return Spell(values);
        }
        catch (Exception e) when (e is not Xunit.Sdk.IAssertionException)
        {
            return Failure(e);
        }
    }

    private static string Spell(List<object?> values)
    {
        if (values.Count == 0) return "no row";
        var first = ConformanceValue.Of(values[0]);
        return values.Skip(1).All(v => v is null) ? first : first + " then " + string.Join(",", values.Skip(1).Select(ConformanceValue.Of));
    }

    private static string Failure(Exception e)
    {
        for (var inner = e; inner != null; inner = inner.InnerException)
            if (inner.Message.Contains("'v'") || inner.Message.Contains("'v.")) return "named";
        return $"raw: {e.GetType().Name}";
    }
}

/// <summary>
/// The contract of <c>IStreamReader.ReadBatchesAsync</c>, which the row fast-path leans on: the array of a
/// yielded batch belongs to the consumer and is never written again, and a batch holds at most
/// <c>batchSize</c> rows. Played over a reader that yields <see cref="Rows"/> rows.
/// </summary>
internal static class BatchContract
{
    public const int Rows = 2500;
    public const int BatchSize = 100;

    public static async Task AssertAsync(IStreamReader reader)
    {
        await reader.OpenAsync();

        var seen = new List<(object?[][] Array, string[] Snapshot)>();
        var total = 0;
        await foreach (var batch in reader.ReadBatchesAsync(BatchSize))
        {
            Assert.True(batch.Length <= BatchSize, $"A batch of {batch.Length} rows exceeds the batch size {BatchSize}.");
            Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(batch, out var segment) && segment.Array is not null,
                "A batch must be backed by an array.");
            var array = segment.Array!;
            foreach (var (other, _) in seen)
                Assert.False(ReferenceEquals(other, array), "Two batches share one array.");

            var rows = batch.ToArray();
            total += rows.Length;
            seen.Add((array, rows.Select(Spell).ToArray()));
        }

        Assert.Equal(Rows, total);
        Assert.True(seen.Count > 1, "The cell must span several batches.");
        // A reader that rewrote an earlier batch's array while reading later ones shows here.
        foreach (var (array, snapshot) in seen)
            Assert.Equal(snapshot, array.Take(snapshot.Length).Select(Spell));
    }

    private static string Spell(object?[] row) => string.Join("|", row.Select(ConformanceValue.Of));
}

/// <summary>
/// The read face of the conformance table: reader x mode x type. Both paths of a reader read a cell into
/// the same value, or refuse it naming the column. A cell marked with a lot is known false and is
/// corrected by that lot: it must still be false, so a correction cannot land without entering the gate.
/// </summary>
public sealed class ReadConformanceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "dtpipe-read-face-" + Guid.NewGuid().ToString("N"));

    public ReadConformanceTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private readonly record struct Cell(string Expected, string? KnownFalse = null)
    {
        public static implicit operator Cell(string expected) => new(expected);
    }

    /// <param name="Reader">The reader under test.</param>
    /// <param name="Mode">How the type reaches a text reader: declared, inferred, schema, sampled (inferred from the first row alone) or mixed (inferred from both rows).</param>
    /// <param name="Type">The type's name for the reader: the hint or schema field of a text reader, the first row of a sampled or mixed cell.</param>
    /// <param name="Source">The first row: an SQL expression, a SQLite literal, a text, a JSON value. The second row of a sampled or mixed cell, whose first is <paramref name="Type"/>.</param>
    /// <param name="Expected">What both paths read from the first row, the other rows being null unless it says so.</param>
    private sealed record Row(string Reader, string Mode, string Type, string Source, Cell Expected);

    private static readonly Row[] Table =
    [
        // duck
        new("duck", "", "bool", "true", "Boolean:true"),
        new("duck", "", "int8", "(-5)::TINYINT", "SByte:-5"),
        new("duck", "", "int16", "(-300)::SMALLINT", "Int16:-300"),
        new("duck", "", "int32", "(-70000)::INTEGER", "Int32:-70000"),
        new("duck", "", "int64", "(-5000000000)::BIGINT", "Int64:-5000000000"),
        new("duck", "", "uint8", "200::UTINYINT", "Byte:200"),
        new("duck", "", "uint16", "60000::USMALLINT", "UInt16:60000"),
        new("duck", "", "uint32", "4000000000::UINTEGER", "UInt32:4000000000"),
        new("duck", "", "uint64", "18000000000000000000::UBIGINT", "UInt64:18000000000000000000"),
        new("duck", "", "float", "1.5::FLOAT", "Single:1.5"),
        new("duck", "", "double", "1.5::DOUBLE", "Double:1.5"),
        new("duck", "", "nan", "'NaN'::DOUBLE", "Double:NaN"),
        new("duck", "", "inf", "'Infinity'::DOUBLE", "Double:Infinity"),
        new("duck", "", "decimal(10,2)", "4.2::DECIMAL(10,2)", "Decimal:4.20"),
        new("duck", "", "decimal(38,18)", "4.29::DECIMAL(38,18)", "Decimal:4.290000000000000000"),
        new("duck", "", "string", "'héllo'", "String:héllo"),
        new("duck", "", "binary", "'abc'::BLOB", "Bytes:616263"),
        new("duck", "", "date", "DATE '2024-01-02'", "DateOnly:2024-01-02"),
        new("duck", "", "time", "TIME '10:11:12.123456'", "TimeOnly:10:11:12.1234560"),
        new("duck", "", "timetz", "TIMETZ '10:11:12+02'", "String:10:11:12+02:00"),
        new("duck", "", "timestamp", "TIMESTAMP '2024-01-02 03:04:05.123456'", "DateTime:2024-01-02T03:04:05.1234560"),
        new("duck", "", "timestamp_s", "TIMESTAMP_S '2024-01-02 03:04:05'", "DateTime:2024-01-02T03:04:05.0000000"),
        new("duck", "", "timestamp_ms", "TIMESTAMP_MS '2024-01-02 03:04:05.123'", "DateTime:2024-01-02T03:04:05.1230000"),
        new("duck", "", "timestamp_ns", "TIMESTAMP_NS '2024-01-02 03:04:05.123456789'", "DateTime:2024-01-02T03:04:05.1234567"),
        new("duck", "", "timestamptz", "TIMESTAMPTZ '2024-01-02 03:04:05.123456+02'", "DateTimeOffset:2024-01-02T01:04:05.1234560+00:00"),
        new("duck", "", "interval", "INTERVAL 1 DAY", "named"),
        new("duck", "", "uuid", "UUID '6f9619ff-8b86-d011-b42d-00c04fc964ff'", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("duck", "", "hugeint", "170141183460469231731687303715884105727::HUGEINT", "Int128:170141183460469231731687303715884105727"),
        new("duck", "", "uhugeint", "340282366920938463463374607431768211455::UHUGEINT", "UInt128:340282366920938463463374607431768211455"),
        new("duck", "", "bit", "'101'::BIT", "String:101"),
        new("duck", "", "list", "[1,2,3]", "[Int32:1,Int32:2,Int32:3]"),
        new("duck", "", "array", "[1,2,3]::INTEGER[3]", "[Int32:1,Int32:2,Int32:3]"),
        new("duck", "", "struct", "{'a':1,'b':'x'}", "{a=Int32:1,b=String:x}"),
        new("duck", "", "map", "MAP{'a':1}", "{a=Int32:1}"),
        new("duck", "", "union", "union_value(a := 1)::UNION(a INTEGER, b VARCHAR)", "named"),
        new("duck", "", "enum", "'a'::ENUM('a','b')", "String:a"),
        new("duck", "", "list<struct>", "[{'a':1}]", "[{a=Int32:1}]"),
        new("duck", "", "list<decimal>", "[4.29::DECIMAL(10,2)]", "[Decimal:4.29]"),
        new("duck", "", "list<timestamp>", "[TIMESTAMP '2024-01-02 03:04:05']", "[DateTime:2024-01-02T03:04:05.0000000]"),
        new("duck", "", "list<uuid>", "[UUID '6f9619ff-8b86-d011-b42d-00c04fc964ff']", "[Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff]"),
        new("duck", "", "struct<uuid>", "{'u':UUID '6f9619ff-8b86-d011-b42d-00c04fc964ff'}", "{u=Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff}"),
        // parquet
        new("parquet", "", "bool", "true", "Boolean:true"),
        new("parquet", "", "int8", "(-5)::TINYINT", "SByte:-5"),
        new("parquet", "", "int16", "(-300)::SMALLINT", "Int16:-300"),
        new("parquet", "", "int32", "(-70000)::INTEGER", "Int32:-70000"),
        new("parquet", "", "int64", "(-5000000000)::BIGINT", "Int64:-5000000000"),
        new("parquet", "", "uint8", "200::UTINYINT", "Byte:200"),
        new("parquet", "", "uint16", "60000::USMALLINT", "UInt16:60000"),
        new("parquet", "", "uint32", "4000000000::UINTEGER", "UInt32:4000000000"),
        new("parquet", "", "uint64", "18000000000000000000::UBIGINT", "UInt64:18000000000000000000"),
        new("parquet", "", "float", "1.5::FLOAT", "Single:1.5"),
        new("parquet", "", "double", "1.5::DOUBLE", "Double:1.5"),
        new("parquet", "", "nan", "'NaN'::DOUBLE", "Double:NaN"),
        new("parquet", "", "inf", "'Infinity'::DOUBLE", "Double:Infinity"),
        new("parquet", "", "decimal(10,2)", "4.2::DECIMAL(10,2)", "Decimal:4.20"),
        new("parquet", "", "decimal(38,18)", "4.29::DECIMAL(38,18)", "Decimal:4.290000000000000000"),
        new("parquet", "", "decimal(38,18)", "12345678901234567.891::DECIMAL(38,18)", "Decimal:12345678901234567.891000000000"),
        new("parquet", "", "decimal(38,18)", "0::DECIMAL(38,18)", "Decimal:0.000000000000000000"),
        new("parquet", "", "string", "'héllo'", "String:héllo"),
        new("parquet", "", "binary", "'abc'::BLOB", "Bytes:616263"),
        new("parquet", "", "date", "DATE '2024-01-02'", "DateOnly:2024-01-02"),
        new("parquet", "", "time", "TIME '10:11:12.123456'", "TimeOnly:10:11:12.1234560"),
        new("parquet", "", "timetz", "TIMETZ '10:11:12+02'", "TimeOnly:08:11:12.0000000"),
        new("parquet", "", "timestamp", "TIMESTAMP '2024-01-02 03:04:05.123456'", "DateTime:2024-01-02T03:04:05.1234560"),
        new("parquet", "", "timestamp_s", "TIMESTAMP_S '2024-01-02 03:04:05'", "DateTime:2024-01-02T03:04:05.0000000"),
        new("parquet", "", "timestamp_ms", "TIMESTAMP_MS '2024-01-02 03:04:05.123'", "DateTime:2024-01-02T03:04:05.1230000"),
        new("parquet", "", "timestamp_ns", "TIMESTAMP_NS '2024-01-02 03:04:05.123456789'", "DateTime:2024-01-02T03:04:05.1234560"),
        new("parquet", "", "timestamptz", "TIMESTAMPTZ '2024-01-02 03:04:05.123456+02'", "DateTimeOffset:2024-01-02T01:04:05.1234560+00:00"),
        new("parquet", "", "interval", "INTERVAL 1 DAY", "named"),
        new("parquet", "", "uuid", "UUID '6f9619ff-8b86-d011-b42d-00c04fc964ff'", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("parquet", "", "bit", "'101'::BIT", "String:101"),
        new("parquet", "", "list", "[1,2,3]", "[Int32:1,Int32:2,Int32:3]"),
        new("parquet", "", "array", "[1,2,3]::INTEGER[3]", "[Int32:1,Int32:2,Int32:3]"),
        new("parquet", "", "struct", "{'a':1,'b':'x'}", "named"),
        new("parquet", "", "map", "MAP{'a':1}", "named"),
        new("parquet", "", "union", "union_value(a := 1)::UNION(a INTEGER, b VARCHAR)", "named"),
        new("parquet", "", "enum", "'a'::ENUM('a','b')", "String:a"),
        new("parquet", "", "list<struct>", "[{'a':1}]", "named"),
        new("parquet", "", "list<decimal>", "[4.29::DECIMAL(10,2)]", "[Decimal:4.29]"),
        new("parquet", "", "list<decimal>", "[4.2::DECIMAL(10,2)]", "[Decimal:4.20]"),
        new("parquet", "", "list<timestamp>", "[TIMESTAMP '2024-01-02 03:04:05']", "[DateTime:2024-01-02T03:04:05.0000000]"),
        new("parquet", "", "list<uuid>", "[UUID '6f9619ff-8b86-d011-b42d-00c04fc964ff']", "named"),
        new("parquet", "", "struct<uuid>", "{'u':UUID '6f9619ff-8b86-d011-b42d-00c04fc964ff'}", "named"),
        // arrow
        new("arrow", "", "null", "", "null"),
        new("arrow", "", "bool", "", "Boolean:true"),
        new("arrow", "", "bool8", "", "Boolean:true"),
        new("arrow", "", "int8", "", "SByte:-5"),
        new("arrow", "", "int16", "", "Int16:-300"),
        new("arrow", "", "int32", "", "Int32:-70000"),
        new("arrow", "", "int64", "", "Int64:-5000000000"),
        new("arrow", "", "uint8", "", "Byte:200"),
        new("arrow", "", "uint16", "", "UInt16:60000"),
        new("arrow", "", "uint32", "", "UInt32:4000000000"),
        new("arrow", "", "uint64", "", "UInt64:18000000000000000000"),
        new("arrow", "", "int128", "", "Int128:170141183460469231731687303715884105727"),
        new("arrow", "", "int128-min", "", "Int128:-170141183460469231731687303715884105728"),
        new("arrow", "", "uint128", "", "UInt128:340282366920938463463374607431768211455"),
        new("arrow", "", "halffloat", "", "named"),
        new("arrow", "", "float", "", "Single:1.5"),
        new("arrow", "", "double", "", "Double:1.5"),
        new("arrow", "", "double-nan", "", "Double:NaN"),
        new("arrow", "", "double-inf", "", "Double:Infinity"),
        new("arrow", "", "double-shortest", "", "Double:0.1"),
        new("arrow", "", "float-shortest", "", "Single:0.1"),
        new("arrow", "", "decimal32", "", "Decimal:4.29"),
        new("arrow", "", "decimal64", "", "Decimal:4.29"),
        new("arrow", "", "decimal128(10,2)", "", "Decimal:4.29"),
        new("arrow", "", "decimal128(38,18)", "", "Decimal:4.290000000000000000"),
        new("arrow", "", "decimal-undeclared", "", "Decimal:4.29"),
        new("arrow", "", "decimal128-wide", "", "Decimal:12345678901234567.891000000000"),
        new("arrow", "", "decimal-undeclared-wide", "", "Decimal:12345678901234567.891"),
        new("arrow", "", "decimal-undeclared-fraction", "", "Decimal:1.23456"),
        new("arrow", "", "decimal256", "", "Decimal:4.2900000000"),
        new("arrow", "", "string", "", "String:héllo"),
        new("arrow", "", "largestring", "", "named"),
        new("arrow", "", "stringview", "", "named"),
        new("arrow", "", "binary", "", "Bytes:616263"),
        new("arrow", "", "largebinary", "", "named"),
        new("arrow", "", "binaryview", "", "named"),
        new("arrow", "", "fixedbinary(8)", "", "Bytes:6162636465666768"),
        new("arrow", "", "uuid", "", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("arrow", "", "date32", "", "DateOnly:2024-01-02"),
        new("arrow", "", "date64", "", "DateTime:2024-01-02T00:00:00.0000000"),
        new("arrow", "", "time32(ms)", "", "TimeOnly:10:11:12.1230000"),
        new("arrow", "", "time64(us)", "", "TimeOnly:10:11:12.1234560"),
        new("arrow", "", "timestamp(s)", "", "DateTime:2024-01-02T03:04:05.0000000"),
        new("arrow", "", "timestamp(us)", "", "DateTime:2024-01-02T03:04:05.1234560"),
        new("arrow", "", "timestamp(ns)", "", "DateTime:2024-01-02T03:04:05.1234560"),
        new("arrow", "", "timestamp(us,UTC)", "", "DateTimeOffset:2024-01-02T03:04:05.1234560+00:00"),
        new("arrow", "", "timestamp(us,Asia/Tokyo)", "", "DateTimeOffset:2024-01-02T03:04:05.1234560+00:00"),
        new("arrow", "", "duration(us)", "", "TimeSpan:00:01:30"),
        new("arrow", "", "interval(ym)", "", "named"),
        new("arrow", "", "interval(dt)", "", "named"),
        new("arrow", "", "interval(mdn)", "", "named"),
        new("arrow", "", "list<int32>", "", "[Int32:1,Int32:2]"),
        new("arrow", "", "list<decimal(10,2)>", "", "[Decimal:4.29]"),
        new("arrow", "", "list<timestamp>", "", "[DateTime:2024-01-02T03:04:05.1234560]"),
        new("arrow", "", "list<uuid>", "", "[Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff]"),
        new("arrow", "", "list<struct>", "", "[{a=Int32:1}]"),
        new("arrow", "", "largelist", "", "named"),
        new("arrow", "", "fixedlist", "", "[Int32:1,Int32:2]"),
        new("arrow", "", "listview", "", "named"),
        new("arrow", "", "largelistview", "", "named"),
        new("arrow", "", "struct", "", "{a=Int32:1,b=String:x}"),
        new("arrow", "", "map", "", "{a=Int32:1}"),
        new("arrow", "", "union(sparse)", "", "named"),
        new("arrow", "", "union(dense)", "", "named"),
        new("arrow", "", "dictionary", "", "String:a"),
        new("arrow", "", "runend", "", "named"),
        // sqlite
        new("sqlite", "", "INTEGER", "42", "Int64:42"),
        new("sqlite", "", "BIGINT", "5000000000", "Int64:5000000000"),
        new("sqlite", "", "REAL", "1.5", "Double:1.5"),
        new("sqlite", "", "TEXT", "'héllo'", "String:héllo"),
        new("sqlite", "", "BLOB", "x'616263'", "Bytes:616263"),
        new("sqlite", "", "NUMERIC", "4.29", "Double:4.29"),
        new("sqlite", "", "DECIMAL(10,2)", "4.29", "Double:4.29"),
        new("sqlite", "", "BOOLEAN", "1", "Int64:1"),
        new("sqlite", "", "DATETIME", "'2024-01-02 03:04:05'", "String:2024-01-02 03:04:05"),
        new("sqlite", "", "DATE", "'2024-01-02'", "String:2024-01-02"),
        new("sqlite", "", "", "'x'", "String:x"),
        new("sqlite", "", "REAL", "'abc'", "String:abc"),
        new("sqlite", "", "INTEGER", "'abc'", "String:abc"),
        // csv declared
        new("csv", "declared", "string", "héllo", "String:héllo then String:"),
        new("csv", "declared", "int32", "42", "Int32:42"),
        new("csv", "declared", "int64", "5000000000", "Int64:5000000000"),
        new("csv", "declared", "double", "1.5", "Double:1.5"),
        new("csv", "declared", "float", "1.5", "Single:1.5"),
        new("csv", "declared", "decimal", "4.29", "Decimal:4.29"),
        new("csv", "declared", "decimal", "12345678901234567.891", "Decimal:12345678901234567.891"),
        new("csv", "declared", "decimal", "1,5", "named"),
        new("csv", "declared", "decimal", "1,000", "named"),
        new("csv", "declared", "decimal", "1,000.50", "named"),
        new("csv", "declared", "double", "1e400", "named"),
        new("csv", "declared", "decimal", "0.1234567890123456789012345678901234", "named"),
        new("csv", "declared", "decimal", "1.5000000000000000000000000000000000", "Decimal:1.5"),
        new("csv", "declared", "decimal", "10.50", "Decimal:10.5"),
        new("csv", "declared", "decimal", "0.0000000000000000000001", "named"),
        new("csv", "declared", "decimal", "1.5000000000000000000000", "Decimal:1.5"),
        new("csv", "declared", "decimal", "170141183460469231732", "named"),
        new("csv", "declared", "bool", "true", "Boolean:true"),
        new("csv", "declared", "bool", "yes", "Boolean:true"),
        new("csv", "declared", "datetime", "2024-01-02T03:04:05", "DateTime:2024-01-02T03:04:05.0000000"),
        new("csv", "declared", "datetime", "2024-01-02T03:04:05.123456", "DateTime:2024-01-02T03:04:05.1234560"),
        new("csv", "declared", "datetime", "2024-01-02T03:04:05+02:00", "DateTime:2024-01-02T01:04:05.0000000"),
        new("csv", "declared", "datetime", "2024-01-02T03:04:05Z", "DateTime:2024-01-02T03:04:05.0000000"),
        new("csv", "declared", "datetimeoffset", "2024-01-02T03:04:05Z", "DateTimeOffset:2024-01-02T03:04:05.0000000+00:00"),
        new("csv", "declared", "datetimeoffset", "2024-01-02T03:04:05", "DateTimeOffset:2024-01-02T03:04:05.0000000+00:00"),
        new("csv", "declared", "datetimeoffset", "2024-01-02T03:04:05+02:00", "DateTimeOffset:2024-01-02T01:04:05.0000000+00:00"),
        new("csv", "declared", "uuid", "6f9619ff-8b86-d011-b42d-00c04fc964ff", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("csv", "declared", "int32", "abc", "named"),
        new("csv", "declared", "int64", "abc", "named"),
        new("csv", "declared", "double", "abc", "named"),
        new("csv", "declared", "float", "abc", "named"),
        new("csv", "declared", "decimal", "abc", "named"),
        new("csv", "declared", "bool", "abc", "named"),
        new("csv", "declared", "datetime", "abc", "named"),
        new("csv", "declared", "datetimeoffset", "abc", "named"),
        new("csv", "declared", "uuid", "abc", "named"),
        new("csv", "declared", "double", "NaN", "Double:NaN"),
        new("csv", "declared", "double", "Infinity", "Double:Infinity"),
        // jsonl declared
        new("jsonl", "declared", "string", "\"héllo\"", "String:héllo"),
        new("jsonl", "declared", "int32", "42", "Int32:42"),
        new("jsonl", "declared", "int64", "5000000000", "Int64:5000000000"),
        new("jsonl", "declared", "double", "1.5", "Double:1.5"),
        new("jsonl", "declared", "float", "1.5", "Single:1.5"),
        new("jsonl", "declared", "decimal", "4.29", "Decimal:4.29"),
        new("jsonl", "declared", "decimal", "12345678901234567.891", "Decimal:12345678901234567.891"),
        new("jsonl", "declared", "decimal", "\"1,5\"", "named"),
        new("jsonl", "declared", "decimal", "\"1,000\"", "named"),
        new("jsonl", "declared", "double", "\"1,000\"", "named"),
        new("jsonl", "declared", "decimal", "0.1234567890123456789012345678901234", "named"),
        new("jsonl", "declared", "decimal", "\"0.1234567890123456789012345678901234\"", "named"),
        new("jsonl", "declared", "decimal", "1e-30", "named"),
        new("jsonl", "declared", "decimal", "10.50", "Decimal:10.5"),
        new("jsonl", "declared", "decimal", "1.5000000000000000000000", "Decimal:1.5"),
        new("jsonl", "declared", "decimal", "170141183460469231732", "named"),
        new("jsonl", "declared", "decimal", "0.0000000000000000000001", "named"),
        new("jsonl", "declared", "bool", "true", "Boolean:true"),
        new("jsonl", "declared", "bool", "\"yes\"", "Boolean:true"),
        new("jsonl", "declared", "datetime", "\"2024-01-02T03:04:05\"", "DateTime:2024-01-02T03:04:05.0000000"),
        new("jsonl", "declared", "datetime", "\"2024-01-02T03:04:05.123456\"", "DateTime:2024-01-02T03:04:05.1234560"),
        new("jsonl", "declared", "datetime", "\"2024-01-02T03:04:05+02:00\"", "DateTime:2024-01-02T01:04:05.0000000"),
        new("jsonl", "declared", "datetime", "\"2024-01-02T03:04:05Z\"", "DateTime:2024-01-02T03:04:05.0000000"),
        new("jsonl", "declared", "datetimeoffset", "\"2024-01-02T03:04:05Z\"", "DateTimeOffset:2024-01-02T03:04:05.0000000+00:00"),
        new("jsonl", "declared", "datetimeoffset", "\"2024-01-02T03:04:05\"", "DateTimeOffset:2024-01-02T03:04:05.0000000+00:00"),
        new("jsonl", "declared", "datetimeoffset", "\"2024-01-02T03:04:05+02:00\"", "DateTimeOffset:2024-01-02T01:04:05.0000000+00:00"),
        new("jsonl", "declared", "uuid", "\"6f9619ff-8b86-d011-b42d-00c04fc964ff\"", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("jsonl", "declared", "int32", "\"abc\"", "named"),
        new("jsonl", "declared", "int64", "\"abc\"", "named"),
        new("jsonl", "declared", "double", "\"abc\"", "named"),
        new("jsonl", "declared", "float", "\"abc\"", "named"),
        new("jsonl", "declared", "decimal", "\"abc\"", "named"),
        new("jsonl", "declared", "bool", "\"abc\"", "named"),
        new("jsonl", "declared", "datetime", "\"abc\"", "named"),
        new("jsonl", "declared", "datetimeoffset", "\"abc\"", "named"),
        new("jsonl", "declared", "uuid", "\"abc\"", "named"),
        new("jsonl", "declared", "double", "\"NaN\"", "Double:NaN"),
        new("jsonl", "declared", "double", "\"Infinity\"", "Double:Infinity"),
        // xml declared
        new("xml", "declared", "string", "héllo", "String:héllo"),
        new("xml", "declared", "int32", "42", "Int32:42"),
        new("xml", "declared", "int64", "5000000000", "Int64:5000000000"),
        new("xml", "declared", "double", "1.5", "Double:1.5"),
        new("xml", "declared", "float", "1.5", "Single:1.5"),
        new("xml", "declared", "decimal", "4.29", "Decimal:4.29"),
        new("xml", "declared", "decimal", "12345678901234567.891", "Decimal:12345678901234567.891"),
        new("xml", "declared", "decimal", "1,5", "named"),
        new("xml", "declared", "decimal", "1,000", "named"),
        new("xml", "declared", "decimal", "10.50", "Decimal:10.5"),
        new("xml", "declared", "decimal", "1.5000000000000000000000", "Decimal:1.5"),
        new("xml", "declared", "double", "1e400", "named"),
        new("xml", "declared", "decimal", "0.1234567890123456789012345678901234", "named"),
        new("xml", "declared", "decimal", "0.0000000000000000000001", "named"),
        new("xml", "declared", "bool", "true", "Boolean:true"),
        new("xml", "declared", "bool", "yes", "Boolean:true"),
        new("xml", "declared", "datetime", "2024-01-02T03:04:05", "DateTime:2024-01-02T03:04:05.0000000"),
        new("xml", "declared", "datetime", "2024-01-02T03:04:05.123456", "DateTime:2024-01-02T03:04:05.1234560"),
        new("xml", "declared", "datetime", "2024-01-02T03:04:05+02:00", "DateTime:2024-01-02T01:04:05.0000000"),
        new("xml", "declared", "datetime", "2024-01-02T03:04:05Z", "DateTime:2024-01-02T03:04:05.0000000"),
        new("xml", "declared", "datetimeoffset", "2024-01-02T03:04:05Z", "DateTimeOffset:2024-01-02T03:04:05.0000000+00:00"),
        new("xml", "declared", "datetimeoffset", "2024-01-02T03:04:05", "DateTimeOffset:2024-01-02T03:04:05.0000000+00:00"),
        new("xml", "declared", "datetimeoffset", "2024-01-02T03:04:05+02:00", "DateTimeOffset:2024-01-02T01:04:05.0000000+00:00"),
        new("xml", "declared", "uuid", "6f9619ff-8b86-d011-b42d-00c04fc964ff", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("xml", "declared", "int32", "abc", "named"),
        new("xml", "declared", "int64", "abc", "named"),
        new("xml", "declared", "double", "abc", "named"),
        new("xml", "declared", "float", "abc", "named"),
        new("xml", "declared", "decimal", "abc", "named"),
        new("xml", "declared", "bool", "abc", "named"),
        new("xml", "declared", "datetime", "abc", "named"),
        new("xml", "declared", "datetimeoffset", "abc", "named"),
        new("xml", "declared", "uuid", "abc", "named"),
        new("xml", "declared", "double", "NaN", "Double:NaN"),
        new("xml", "declared", "double", "Infinity", "Double:Infinity"),
        // csv inferred
        new("csv", "inferred", "string", "héllo", "String:héllo then String:"),
        new("csv", "inferred", "int32", "42", "Int32:42"),
        new("csv", "inferred", "int64", "5000000000", "Int64:5000000000"),
        new("csv", "inferred", "double", "1.5", "Double:1.5"),
        new("csv", "inferred", "float", "1.5", "Double:1.5"),
        new("csv", "inferred", "decimal", "4.29", "Double:4.29"),
        new("csv", "inferred", "decimal", "12345678901234567.891", "Decimal:12345678901234567.891"),
        new("csv", "inferred", "decimal", "1,5", "String:1,5 then String:"),
        new("csv", "inferred", "decimal", "1,000.50", "String:1,000.50 then String:"),
        new("csv", "inferred", "double", "1e400", "String:1e400 then String:"),
        new("csv", "inferred", "decimal", "0.1234567890123456789012345678901234", "named"),
        new("csv", "inferred", "double", "0.30000000000000004", "Double:0.30000000000000004"),
        new("csv", "inferred", "double", "0.00012345678901234567", "Double:0.00012345678901234567"),
        new("csv", "inferred", "decimal", "0.10000000000000001", "Decimal:0.10000000000000001"),
        new("csv", "inferred", "decimal", "0.1000000000000000000000000000000001", "named"),
        new("csv", "inferred", "decimal", "12345678901234567890", "Decimal:12345678901234567890"),
        new("csv", "inferred", "decimal", "1.50-", "Decimal:-1.5"),
        new("csv", "inferred", "string", "5-", "String:5- then String:"),
        new("csv", "inferred", "string", "1234567890123456789012345678901234567890", "String:1234567890123456789012345678901234567890 then String:"),
        new("csv", "inferred", "string", "1.2345678901234567890e5", "String:1.2345678901234567890e5 then String:"),
        new("csv", "inferred", "string", "1e-400", "String:1e-400 then String:"),
        new("csv", "mixed", "1", "12345678901234567.891", "Decimal:1 then Decimal:12345678901234567.891"),
        new("csv", "mixed", "1.5", "12345678901234567.891", "Decimal:1.5 then Decimal:12345678901234567.891"),
        new("csv", "inferred", "bool", "true", "Boolean:true"),
        new("csv", "inferred", "bool", "yes", "Boolean:true"),
        new("csv", "inferred", "datetime", "2024-01-02T03:04:05", "DateTime:2024-01-02T03:04:05.0000000"),
        new("csv", "inferred", "datetime", "2024-01-02T03:04:05.123456", "DateTime:2024-01-02T03:04:05.1234560"),
        new("csv", "inferred", "datetime", "2024-01-02T03:04:05+02:00", "DateTime:2024-01-02T01:04:05.0000000"),
        new("csv", "inferred", "datetime", "2024-01-02T03:04:05Z", "DateTime:2024-01-02T03:04:05.0000000"),
        new("csv", "inferred", "datetimeoffset", "2024-01-02T03:04:05Z", "DateTime:2024-01-02T03:04:05.0000000"),
        new("csv", "inferred", "datetimeoffset", "2024-01-02T03:04:05", "DateTime:2024-01-02T03:04:05.0000000"),
        new("csv", "inferred", "datetimeoffset", "2024-01-02T03:04:05+02:00", "DateTime:2024-01-02T01:04:05.0000000"),
        new("csv", "inferred", "uuid", "6f9619ff-8b86-d011-b42d-00c04fc964ff", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("csv", "inferred", "int32", "abc", "String:abc then String:"),
        new("csv", "inferred", "int64", "abc", "String:abc then String:"),
        new("csv", "inferred", "double", "abc", "String:abc then String:"),
        new("csv", "inferred", "float", "abc", "String:abc then String:"),
        new("csv", "inferred", "decimal", "abc", "String:abc then String:"),
        new("csv", "inferred", "bool", "abc", "String:abc then String:"),
        new("csv", "inferred", "datetime", "abc", "String:abc then String:"),
        new("csv", "inferred", "datetimeoffset", "abc", "String:abc then String:"),
        new("csv", "inferred", "uuid", "abc", "String:abc then String:"),
        new("csv", "inferred", "double", "NaN", "Double:NaN"),
        new("csv", "inferred", "double", "Infinity", "Double:Infinity"),
        // jsonl inferred
        new("jsonl", "inferred", "string", "\"héllo\"", "String:héllo"),
        new("jsonl", "inferred", "int32", "42", "Int64:42"),
        new("jsonl", "inferred", "int64", "5000000000", "Int64:5000000000"),
        new("jsonl", "inferred", "double", "1.5", "Double:1.5"),
        new("jsonl", "inferred", "decimal", "0.1234567890123456789012345678901234", "named"),
        new("jsonl", "inferred", "double", "0.00012345678901234567", "Double:0.00012345678901234567"),
        new("jsonl", "inferred", "double", "1.5e-25", "Double:1.5E-25"),
        new("jsonl", "inferred", "double", "0.30000000000000004", "Double:0.30000000000000004"),
        new("jsonl", "inferred", "decimal", "0.10000000000000001", "Decimal:0.10000000000000001"),
        new("jsonl", "inferred", "decimal", "0.1000000000000000000000000000000001", "named"),
        new("jsonl", "inferred", "decimal", "12345678901234567890", "Decimal:12345678901234567890"),
        new("jsonl", "inferred", "string", "1234567890123456789012345678901234567890", "String:1234567890123456789012345678901234567890"),
        new("jsonl", "inferred", "string", "1e400", "String:1e400"),
        new("jsonl", "inferred", "string", "1e-400", "String:1e-400"),
        new("jsonl", "inferred", "string", "1.2345678901234567890e5", "String:1.2345678901234567890e5"),
        new("jsonl", "mixed", "1.5", "12345678901234567.891", "Decimal:1.5 then Decimal:12345678901234567.891"),
        new("jsonl", "inferred", "float", "1.5", "Double:1.5"),
        new("jsonl", "inferred", "decimal", "4.29", "Double:4.29"),
        new("jsonl", "inferred", "decimal", "12345678901234567.891", "Decimal:12345678901234567.891"),
        new("jsonl", "inferred", "decimal", "\"1,5\"", "String:1,5"),
        new("jsonl", "inferred", "bool", "true", "Boolean:true"),
        new("jsonl", "inferred", "bool", "\"yes\"", "String:yes"),
        new("jsonl", "inferred", "datetime", "\"2024-01-02T03:04:05\"", "String:2024-01-02T03:04:05"),
        new("jsonl", "inferred", "datetime", "\"2024-01-02T03:04:05.123456\"", "String:2024-01-02T03:04:05.123456"),
        new("jsonl", "inferred", "datetime", "\"2024-01-02T03:04:05+02:00\"", "String:2024-01-02T03:04:05+02:00"),
        new("jsonl", "inferred", "datetime", "\"2024-01-02T03:04:05Z\"", "String:2024-01-02T03:04:05Z"),
        new("jsonl", "inferred", "datetimeoffset", "\"2024-01-02T03:04:05Z\"", "String:2024-01-02T03:04:05Z"),
        new("jsonl", "inferred", "datetimeoffset", "\"2024-01-02T03:04:05\"", "String:2024-01-02T03:04:05"),
        new("jsonl", "inferred", "datetimeoffset", "\"2024-01-02T03:04:05+02:00\"", "String:2024-01-02T03:04:05+02:00"),
        new("jsonl", "inferred", "uuid", "\"6f9619ff-8b86-d011-b42d-00c04fc964ff\"", "String:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("jsonl", "inferred", "int32", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "int64", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "double", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "float", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "decimal", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "bool", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "datetime", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "datetimeoffset", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "uuid", "\"abc\"", "String:abc"),
        new("jsonl", "inferred", "double", "\"NaN\"", "String:NaN"),
        new("jsonl", "inferred", "double", "\"Infinity\"", "String:Infinity"),
        // xml inferred
        new("xml", "inferred", "string", "héllo", "String:héllo"),
        new("xml", "inferred", "int32", "42", "Int32:42"),
        new("xml", "inferred", "int64", "5000000000", "Int64:5000000000"),
        new("xml", "inferred", "double", "1.5", "Double:1.5"),
        new("xml", "inferred", "float", "1.5", "Double:1.5"),
        new("xml", "inferred", "decimal", "4.29", "Double:4.29"),
        new("xml", "inferred", "decimal", "12345678901234567.891", "Decimal:12345678901234567.891"),
        new("xml", "inferred", "decimal", "0.1234567890123456789012345678901234", "named"),
        new("xml", "inferred", "double", "0.30000000000000004", "Double:0.30000000000000004"),
        new("xml", "inferred", "double", "0.00012345678901234567", "Double:0.00012345678901234567"),
        new("xml", "inferred", "decimal", "0.10000000000000001", "Decimal:0.10000000000000001"),
        new("xml", "inferred", "decimal", "0.1000000000000000000000000000000001", "named"),
        new("xml", "inferred", "decimal", "12345678901234567890", "Decimal:12345678901234567890"),
        new("xml", "inferred", "string", "1234567890123456789012345678901234567890", "String:1234567890123456789012345678901234567890"),
        new("xml", "inferred", "string", "1e-400", "String:1e-400"),
        new("xml", "mixed", "1", "12345678901234567.891", "Decimal:1 then Decimal:12345678901234567.891"),
        new("xml", "inferred", "decimal", "1,5", "String:1,5"),
        new("xml", "inferred", "bool", "true", "Boolean:true"),
        new("xml", "inferred", "bool", "yes", "Boolean:true"),
        new("xml", "inferred", "datetime", "2024-01-02T03:04:05", "DateTime:2024-01-02T03:04:05.0000000"),
        new("xml", "inferred", "datetime", "2024-01-02T03:04:05.123456", "DateTime:2024-01-02T03:04:05.1234560"),
        new("xml", "inferred", "datetime", "2024-01-02T03:04:05+02:00", "DateTime:2024-01-02T01:04:05.0000000"),
        new("xml", "inferred", "datetime", "2024-01-02T03:04:05Z", "DateTime:2024-01-02T03:04:05.0000000"),
        new("xml", "inferred", "datetimeoffset", "2024-01-02T03:04:05Z", "DateTime:2024-01-02T03:04:05.0000000"),
        new("xml", "inferred", "datetimeoffset", "2024-01-02T03:04:05", "DateTime:2024-01-02T03:04:05.0000000"),
        new("xml", "inferred", "datetimeoffset", "2024-01-02T03:04:05+02:00", "DateTime:2024-01-02T01:04:05.0000000"),
        new("xml", "inferred", "uuid", "6f9619ff-8b86-d011-b42d-00c04fc964ff", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("xml", "inferred", "int32", "abc", "String:abc"),
        new("xml", "inferred", "int64", "abc", "String:abc"),
        new("xml", "inferred", "double", "abc", "String:abc"),
        new("xml", "inferred", "float", "abc", "String:abc"),
        new("xml", "inferred", "decimal", "abc", "String:abc"),
        new("xml", "inferred", "bool", "abc", "String:abc"),
        new("xml", "inferred", "datetime", "abc", "String:abc"),
        new("xml", "inferred", "datetimeoffset", "abc", "String:abc"),
        new("xml", "inferred", "uuid", "abc", "String:abc"),
        new("xml", "inferred", "double", "NaN", "Double:NaN"),
        new("xml", "inferred", "double", "Infinity", "Double:Infinity"),
        // csv sampled
        new("csv", "sampled", "1", "abc", "named"),
        new("csv", "sampled", "1.5", "1,5", "named"),
        new("csv", "sampled", "true", "abc", "named"),
        new("csv", "sampled", "1", "1.5", "named"),
        new("csv", "sampled", "1", "x", "named"),
        // jsonl sampled
        new("jsonl", "sampled", "1", "\"abc\"", "named"),
        new("jsonl", "sampled", "1.5", "\"1,5\"", "named"),
        new("jsonl", "sampled", "true", "\"abc\"", "named"),
        new("jsonl", "sampled", "1", "1.5", "named"),
        new("jsonl", "sampled", "[1,2]", "\"x\"", "named"),
        new("jsonl", "sampled", """{"a":1}""", "\"x\"", "named"),
        new("jsonl", "sampled", "[1,2]", "5", "named"),
        new("jsonl", "sampled", "[1,2]", """{"a":1}""", "named"),
        new("jsonl", "sampled", """["a"]""", "\"x\"", "named"),
        new("jsonl", "sampled", """{"a":1}""", "5", "named"),
        new("jsonl", "sampled", """{"a":1}""", "[1]", "named"),
        new("jsonl", "sampled", """{"a":"b"}""", "5", "named"),
        // xml sampled
        new("xml", "sampled", "1", "abc", "named"),
        new("xml", "sampled", "1.5", "1,5", "named"),
        new("xml", "sampled", "true", "abc", "named"),
        new("xml", "sampled", "1", "1.5", "named"),
        new("xml", "sampled", "1", "x", "named"),
        // jsonl schema
        new("jsonl", "schema", "decimal(10,2)", "4.2", "Decimal:4.20"),
        new("jsonl", "schema", "decimal(10,2)", "1.234", "named"),
        new("jsonl", "schema", "decimal(10,2)", "1.2300", "Decimal:1.23"),
        new("jsonl", "schema", "decimal(10,2)", "123456789.5", "named"),
        new("jsonl", "schema", "timestamp(us)", "\"2024-01-02T03:04:05\"", "DateTime:2024-01-02T03:04:05.0000000"),
        new("jsonl", "schema", "timestamp(us,UTC)", "\"2024-01-02T03:04:05Z\"", "DateTimeOffset:2024-01-02T03:04:05.0000000+00:00"),
        new("jsonl", "schema", "uuid", "\"6f9619ff-8b86-d011-b42d-00c04fc964ff\"", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("jsonl", "schema", "int32", "42", "Int32:42"),
        new("jsonl", "schema", "int32-comma", "\"1,5\"", "named"),
        new("jsonl", "schema", "double-comma", "\"1,5\"", "named"),
        new("jsonl", "schema", "date32", "\"2024-01-02\"", "DateOnly:2024-01-02"),
        new("jsonl", "schema", "date32", "\"x\"", "named"),
        new("jsonl", "schema", "time64", "\"10:11:12.123456\"", "TimeOnly:10:11:12.1234560"),
        new("jsonl", "schema", "list<int64>", "[1,2]", "[Int64:1,Int64:2]"),
        new("jsonl", "schema", "list<int64>", "\"x\"", "named"),
        new("jsonl", "schema", "struct<a>", "5", "named"),
        // xml schema
        new("xml", "schema", "decimal(10,2)", "4.2", "Decimal:4.20"),
        new("xml", "schema", "decimal(10,2)", "1.234", "named"),
        new("xml", "schema", "timestamp(us)", "2024-01-02T03:04:05", "DateTime:2024-01-02T03:04:05.0000000"),
        new("xml", "schema", "timestamp(us,UTC)", "2024-01-02T03:04:05Z", "DateTimeOffset:2024-01-02T03:04:05.0000000+00:00"),
        new("xml", "schema", "uuid", "6f9619ff-8b86-d011-b42d-00c04fc964ff", "Guid:6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        new("xml", "schema", "int32", "42", "Int32:42"),
        new("xml", "schema", "int32-comma", "1,5", "named"),
        new("xml", "schema", "double-comma", "1,5", "named"),
        new("xml", "schema", "date32", "2024-01-02", "DateOnly:2024-01-02"),
        new("xml", "schema", "date32", "x", "named"),
        new("xml", "schema", "time64", "10:11:12.123456", "TimeOnly:10:11:12.1234560"),
    ];

    private static readonly IReadOnlyDictionary<string, Field> SchemaFields = new Dictionary<string, Field>
    {
        ["decimal(10,2)"] = new("v", new Decimal128Type(10, 2), true),
        ["timestamp(us)"] = new("v", new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, (string?)null), true),
        ["timestamp(us,UTC)"] = new("v", new TimestampType(Apache.Arrow.Types.TimeUnit.Microsecond, "UTC"), true),
        ["uuid"] = ArrowTypeCases.Named("uuid").Field,
        ["int32"] = new("v", Int32Type.Default, true),
        ["int32-comma"] = new("v", Int32Type.Default, true),
        ["double-comma"] = new("v", DoubleType.Default, true),
        ["date32"] = new("v", Date32Type.Default, true),
        ["time64"] = new("v", new Time64Type(Apache.Arrow.Types.TimeUnit.Microsecond), true),
        ["list<int64>"] = new("v", new ListType(Int64Type.Default), true),
        ["struct<a>"] = new("v", new StructType([new Field("a", Int64Type.Default, true)]), true),
    };

    public static TheoryData<string, string, string, string> Cells()
    {
        var cells = new TheoryData<string, string, string, string>();
        foreach (var row in Table) cells.Add(row.Reader, row.Mode, row.Type, row.Source);
        return cells;
    }

    [Theory]
    [MemberData(nameof(Cells))]
    public async Task A_Reader_Reads_A_Cell_Alike_On_Both_Paths(string reader, string mode, string type, string source)
    {
        var row = Table.Single(r => r.Reader == reader && r.Mode == mode && r.Type == type && r.Source == source);
        var open = Open(row);

        var columnar = await ReadOutcome.ColumnarAsync(open);
        var rows = await ReadOutcome.RowsAsync(open);

        var expected = row.Expected.Expected;
        if (row.Expected.KnownFalse is { } lot)
        {
            Assert.False(rows == expected && (columnar is null || columnar == expected),
                $"This cell now reads '{rows}': the correction of {lot} is in, drop its mark.");
            return;
        }

        Assert.Equal(expected, rows);
        if (columnar is not null) Assert.Equal(expected, columnar);
    }

    [Fact]
    public void Every_Arrow_Case_Has_A_Row_On_The_Arrow_Reader() => Assert.Equal(
        ArrowTypeCases.All.Select(c => c.Name).Order(StringComparer.Ordinal),
        Table.Where(r => r.Reader == "arrow").Select(r => r.Type).Order(StringComparer.Ordinal));

    /// <summary>
    /// A scalar beside an object in the sample is the struct's <c>_value</c>, on both paths, where any other
    /// scalar in a struct is refused. An absent field reads as null, as the column's Arrow form gives it back.
    /// </summary>
    [Fact]
    public async Task A_Scalar_Beside_An_Object_In_The_Sample_Is_The_Value_Field()
    {
        var path = Path.Combine(_dir, "merged.jsonl");
        File.WriteAllText(path, "{\"id\":1,\"v\":{\"a\":1}}\n{\"id\":2,\"v\":5}\n");
        IStreamReader Open() => new JsonLStreamReader(path, new JsonLReaderOptions { AutoColumnTypes = true });

        Assert.Equal("{_value=null,a=Int64:1} then {_value=Int64:5,a=null}", await ReadOutcome.ColumnarAsync(Open));
        Assert.Equal("{_value=null,a=Int64:1} then {_value=Int64:5,a=null}", await ReadOutcome.RowsAsync(Open));
    }

    /// <summary>A struct holds every field of its type on both paths, an absent one as null.</summary>
    [Fact]
    public async Task An_Absent_Field_Of_A_Struct_Reads_As_Null_On_Both_Paths()
    {
        var xml = Path.Combine(_dir, "sparse.xml");
        File.WriteAllText(xml, "<rows><r><id>1</id><v><a>1</a></v></r><r><id>2</id><v><b>2</b></v></r></rows>");
        IStreamReader OpenXml() => new XmlStreamReader(xml, new XmlReaderOptions { Path = "/rows/r", AutoColumnTypes = true });

        var jsonl = Path.Combine(_dir, "sparse.jsonl");
        File.WriteAllText(jsonl, "{\"id\":1,\"v\":{\"a\":1}}\n{\"id\":2,\"v\":{\"b\":2}}\n");
        IStreamReader OpenJsonl() => new JsonLStreamReader(jsonl, new JsonLReaderOptions { AutoColumnTypes = true });

        Assert.Equal("{a=Int32:1,b=null} then {a=null,b=Int32:2}", await ReadOutcome.RowsAsync(OpenXml));
        Assert.Equal(await ReadOutcome.ColumnarAsync(OpenXml), await ReadOutcome.RowsAsync(OpenXml));
        Assert.Equal("{a=Int64:1,b=null} then {a=null,b=Int64:2}", await ReadOutcome.RowsAsync(OpenJsonl));
        Assert.Equal(await ReadOutcome.ColumnarAsync(OpenJsonl), await ReadOutcome.RowsAsync(OpenJsonl));
    }

    /// <summary>A query failing once its first rows are out is a failure on both paths, never the end of its rows.</summary>
    [Fact]
    public async Task A_Query_Failing_Mid_Stream_Is_Not_The_End_Of_Its_Rows()
    {
        const string query = "SELECT CASE WHEN i < 200000 THEN i ELSE error('boom') END AS v FROM range(400000) t(i)";
        IStreamReader Open() => new DuckDataSourceReader("Data Source=:memory:", query, new DuckDbReaderOptions());

        Assert.Equal("raw: InvalidOperationException", await ReadOutcome.ColumnarAsync(Open));
        Assert.Equal("raw: InvalidOperationException", await ReadOutcome.RowsAsync(Open));
    }

    private const int ContractRows = BatchContract.Rows;

    public static TheoryData<string> BatchContractReaders() =>
        ["csv", "jsonl", "xml", "sqlite", "duck", "parquet", "arrow", "generate", "memory-channel"];

    private Func<IStreamReader> OpenManyRows(string reader)
    {
        var path = Path.Combine(_dir, Guid.NewGuid().ToString("N") + "." + reader);
        var query = $"SELECT i AS id, 'row ' || i AS v FROM range({ContractRows}) t(i)";
        switch (reader)
        {
            case "csv":
                File.WriteAllLines(path, ["id,v", .. Enumerable.Range(0, ContractRows).Select(i => $"{i},row {i}")]);
                return () => new CsvStreamReader(path, new CsvReaderOptions());
            case "jsonl":
                File.WriteAllLines(path, Enumerable.Range(0, ContractRows).Select(i => $"{{\"id\":{i},\"v\":\"row {i}\"}}"));
                return () => new JsonLStreamReader(path, new JsonLReaderOptions { AutoColumnTypes = true });
            case "xml":
                File.WriteAllText(path, "<rows>" + string.Concat(Enumerable.Range(0, ContractRows).Select(i => $"<r><id>{i}</id><v>row {i}</v></r>")) + "</rows>");
                return () => new XmlStreamReader(path, new XmlReaderOptions { Path = "/rows/r" });
            case "sqlite":
                using (var connection = new SqliteConnection($"Data Source={path};Pooling=False"))
                using (var command = connection.CreateCommand())
                {
                    connection.Open();
                    command.CommandText = $"CREATE TABLE t(id INTEGER, v TEXT); WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < {ContractRows - 1}) INSERT INTO t SELECT i, 'row ' || i FROM n;";
                    command.ExecuteNonQuery();
                }
                return () => new SqliteReader($"Data Source={path}", "SELECT id, v FROM t ORDER BY id");
            case "duck":
                return () => new DuckDataSourceReader("Data Source=:memory:", query, new DuckDbReaderOptions());
            case "parquet":
                using (var connection = new DuckDBConnection("DataSource=:memory:"))
                using (var command = connection.CreateCommand())
                {
                    connection.Open();
                    command.CommandText = $"COPY ({query}) TO '{path}' (FORMAT parquet)";
                    command.ExecuteNonQuery();
                }
                return () => new ParquetStreamReader(path);
            case "arrow":
                var ids = new Int64Array.Builder();
                for (var i = 0; i < ContractRows; i++) ids.Append(i);
                using (var batch = new RecordBatch(new Schema([new Field("id", Int64Type.Default, false)], null), [ids.Build()], ContractRows))
                using (var stream = File.Create(path))
                using (var writer = new ArrowFileWriter(stream, batch.Schema))
                {
                    writer.WriteRecordBatch(batch);
                    writer.WriteEnd();
                }
                return () => new ArrowAdapterStreamReader(path, new ArrowReaderOptions());
            case "memory-channel":
                var channel = System.Threading.Channels.Channel.CreateUnbounded<RecordBatch>();
                var channelSchema = new Schema([new Field("id", Int64Type.Default, false)], null);
                for (var half = 0; half < 2; half++)
                {
                    var halfIds = new Int64Array.Builder();
                    for (var i = 0; i < ContractRows / 2; i++) halfIds.Append(half * (ContractRows / 2) + i);
                    channel.Writer.TryWrite(new RecordBatch(channelSchema, [halfIds.Build()], ContractRows / 2));
                }
                channel.Writer.Complete();
                var registry = new DtPipe.Core.Pipelines.Dag.MemoryChannelRegistry();
                registry.RegisterArrowChannel("src", channel, channelSchema);
                registry.UpdateArrowChannelSchema("src", channelSchema);
                return () => new DtPipe.Adapters.MemoryChannel.ArrowMemoryChannelStreamReader(channel.Reader, registry, "src");
            case "generate":
                return () => new GenerateReader($"count={ContractRows}", "", new GenerateReaderOptions());
            default:
                throw new ArgumentOutOfRangeException(nameof(reader), reader, null);
        }
    }

    /// <summary>
    /// The contract of <c>ReadBatchesAsync</c>, which the row fast-path leans on: the array of a yielded batch
    /// belongs to the consumer and is never written again, and a batch holds at most <c>batchSize</c> rows.
    /// </summary>
    [Theory]
    [MemberData(nameof(BatchContractReaders))]
    public async Task A_Row_Reader_Yields_Distinct_Arrays_Of_At_Most_BatchSize_Rows(string readerName)
    {
        await using var reader = OpenManyRows(readerName)();
        await BatchContract.AssertAsync(reader);
    }

    private Func<IStreamReader> Open(Row row)
    {
        var path = Path.Combine(_dir, Guid.NewGuid().ToString("N") + "." + row.Reader);
        var query = $"SELECT 1 AS id, {row.Source} AS v UNION ALL SELECT 2, NULL ORDER BY id";

        switch (row.Reader)
        {
            case "duck":
                return () => new DuckDataSourceReader("Data Source=:memory:", query, new DuckDbReaderOptions());
            case "parquet":
                using (var connection = new DuckDBConnection("DataSource=:memory:"))
                using (var command = connection.CreateCommand())
                {
                    connection.Open();
                    command.CommandText = $"COPY ({query}) TO '{path}' (FORMAT parquet)";
                    command.ExecuteNonQuery();
                }
                return () => new ParquetStreamReader(path);
            case "arrow":
                ArrowTypeCases.Named(row.Type).WriteIpc(path);
                return () => new ArrowAdapterStreamReader(path, new ArrowReaderOptions());
            case "sqlite":
                using (var connection = new SqliteConnection($"Data Source={path};Pooling=False"))
                using (var command = connection.CreateCommand())
                {
                    connection.Open();
                    command.CommandText = $"CREATE TABLE t(id INTEGER, v {row.Type}); INSERT INTO t VALUES (1, {row.Source}), (2, NULL);";
                    command.ExecuteNonQuery();
                }
                return () => new SqliteReader($"Data Source={path}", "SELECT id, v FROM t ORDER BY id");
            default:
                return OpenText(row, path);
        }
    }

    private static Func<IStreamReader> OpenText(Row row, string path)
    {
        var sampled = row.Mode == "sampled";
        var twoRows = row.Mode is "sampled" or "mixed";
        var first = twoRows ? row.Type : row.Source;
        var second = twoRows ? row.Source : null;

        var columnTypes = row.Mode == "declared" ? $"id:int32,v:{row.Type}" : "";
        var inferred = row.Mode is "inferred" or "sampled" or "mixed";
        var schema = row.Mode == "schema"
            ? ArrowSchemaSerializer.SerializeCompact(new Schema([new Field("id", Int32Type.Default, true), SchemaFields[row.Type]], null))
            : "";

        switch (row.Reader)
        {
            case "csv":
                File.WriteAllText(path, $"id,v\n1,{Quoted(first)}\n2,{Quoted(second)}\n");
                return () => WithSample(new CsvReaderOptions { ColumnTypes = columnTypes, AutoColumnTypes = inferred }, sampled, o => new CsvStreamReader(path, o));
            case "jsonl":
                File.WriteAllText(path, $"{{\"id\":1,\"v\":{first}}}\n{{\"id\":2,\"v\":{second ?? "null"}}}\n");
                return () => WithSample(new JsonLReaderOptions { ColumnTypes = columnTypes, AutoColumnTypes = inferred, Schema = schema }, sampled, o => new JsonLStreamReader(path, o));
            case "xml":
                File.WriteAllText(path, $"<rows><r><id>1</id><v>{first}</v></r><r><id>2</id>{(second is null ? "" : $"<v>{second}</v>")}</r></rows>");
                return () => WithSample(new XmlReaderOptions { Path = "/rows/r", ColumnTypes = columnTypes, AutoColumnTypes = inferred, Schema = schema }, sampled, o => new XmlStreamReader(path, o));
            default:
                throw new ArgumentOutOfRangeException(nameof(row), row.Reader, null);
        }
    }

    private static string Quoted(string? text) => text is null ? "" : text.Contains(',') ? $"\"{text}\"" : text;

    private static IStreamReader WithSample<TOptions>(TOptions options, bool sampled, Func<TOptions, IStreamReader> open)
        where TOptions : TextSourceOptions
    {
        if (sampled) options.MaxSample = 1;
        return open(options);
    }
}
