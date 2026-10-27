using DuckDB.NET.Data;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters;

/// <summary>
/// A file writer writes each flat Arrow type under the type its format has for it, or refuses the
/// column before the target exists; it never turns it into text. One row per type, read back by
/// DuckDB, which knows every one of these formats.
/// </summary>
public sealed class WriterFlatTypeTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "dtpipe-flat-types-" + Guid.NewGuid().ToString("N"));

    public WriterFlatTypeTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    public static TheoryData<string, string, string> Cells() => new()
    {
        // writer, DuckDB source expression, "<type the target holds> | <value read back>"
        { "duck", "2::TINYINT", "TINYINT | 2" },
        { "duck", "2::USMALLINT", "USMALLINT | 2" },
        { "duck", "2::UINTEGER", "UINTEGER | 2" },
        { "duck", "2::UBIGINT", "UBIGINT | 2" },
        { "duck", "DATE '2025-09-05'", "DATE | 2025-09-05" },
        { "duck", "TIME '10:00:00.123456'", "TIME | 10:00:00.123456" },
        { "duck", "TIMESTAMPTZ '2025-09-05 03:43:00+00'", "TIMESTAMP WITH TIME ZONE | 2025-09-05 03:43:00+00" },
        { "duck", "170141183460469231731687303715884105727::HUGEINT", "HUGEINT | 170141183460469231731687303715884105727" },
        { "parquet", "2::TINYINT", "TINYINT | 2" },
        { "parquet", "2::SMALLINT", "SMALLINT | 2" },
        { "parquet", "2::UTINYINT", "UTINYINT | 2" },
        { "parquet", "2::USMALLINT", "USMALLINT | 2" },
        { "parquet", "2::UINTEGER", "UINTEGER | 2" },
        { "parquet", "2::UBIGINT", "UBIGINT | 2" },
        { "parquet", "true", "BOOLEAN | true" },
        { "parquet", "DATE '2025-09-05'", "DATE | 2025-09-05" },
        { "parquet", "TIME '10:00:00.123456'", "TIME | 10:00:00.123456" },
        { "parquet", "TIMESTAMPTZ '2025-09-05 03:43:00+00'", "TIMESTAMP WITH TIME ZONE | 2025-09-05 03:43:00+00" },
        { "csv", "DATE '2025-09-05'", "2025-09-05" },
        { "csv", "TIME '10:00:00.123456'", "10:00:00.123456" },
        { "csv", "TIMESTAMPTZ '2025-09-05 03:43:00+00'", "2025-09-05 03:43:00.000000+00:00" },
        { "csv", "(-170141183460469231731687303715884105727)::HUGEINT - 1", "-170141183460469231731687303715884105728" },
        { "jsonl", "2::USMALLINT", "2" },
        { "jsonl", "2::HUGEINT", "2" },
        { "jsonl", "DATE '2025-09-05'", "\"2025-09-05\"" },
        { "jsonl", "TIME '10:00:00.123456'", "\"10:00:00.123456\"" },
    };

    [Theory]
    [MemberData(nameof(Cells))]
    public async Task A_Flat_Type_Is_Written_Under_Its_Own_Type(string writer, string expression, string expected)
    {
        var target = Path.Combine(_dir, "out." + (writer == "duck" ? "duckdb" : writer));
        var output = writer == "duck" ? ["-o", $"duck:{target}", "-t", "t"] : new[] { "-o", $"{writer}:{target}" };

        Assert.Equal(0, await Program.Main(["-i", "duck::memory:", "-q", $"select {expression} as c", .. output, "--no-stats"]));

        Assert.Equal(expected, ReadBack(writer, target));
    }

    [Fact]
    public async Task A_Refused_Column_Leaves_No_File()
    {
        var target = Path.Combine(_dir, "out.db");

        // SQLite's INTEGER is signed 64-bit: an unsigned 64-bit column has no native home there.
        Assert.Equal(1, await Program.Main(["-i", "duck::memory:", "-q", "select 2::UBIGINT as c", "-o", $"sqlite:{target}", "-t", "t", "--no-stats"]));

        Assert.False(File.Exists(target));
    }

    [Fact]
    public async Task A_128_Bit_Integer_Keeps_Its_Type_Through_A_Row_Transformer()
    {
        var target = Path.Combine(_dir, "out.duckdb");

        Assert.Equal(0, await Program.Main(["-i", "duck::memory:", "-q", "select 170141183460469231731687303715884105727::HUGEINT as c, 1 as id",
            "--compute", "id2:row.id", "-o", $"duck:{target}", "-t", "t", "--no-stats"]));

        Assert.Equal("HUGEINT | 170141183460469231731687303715884105727", ReadBack("duck", target));
    }

    [Fact]
    public async Task A_128_Bit_Integer_Is_Refused_Where_The_Format_Has_No_Type_That_Holds_It()
    {
        var target = Path.Combine(_dir, "out.parquet");

        Assert.Equal(1, await Program.Main(["-i", "duck::memory:", "-q", "select 2::HUGEINT as c", "-o", $"parquet:{target}", "--no-stats"]));

        Assert.False(File.Exists(target));
    }

    [Theory]
    [InlineData("2::UHUGEINT")]
    [InlineData("340282366920938463463374607431768211455::UHUGEINT")]
    public async Task An_Unsigned_128_Bit_Integer_Is_Refused_By_Every_Writer_Before_The_Target_Exists(string expression)
    {
        var target = Path.Combine(_dir, "out.duckdb");

        // The DuckDB appender writes a 128-bit integer only up to 2^127 - 1: half of UHUGEINT lies past it.
        Assert.Equal(1, await Program.Main(["-i", "duck::memory:", "-q", $"select {expression} as c", "-o", $"duck:{target}", "-t", "t", "--no-stats"]));

        Assert.False(File.Exists(target));
    }

    private static string ReadBack(string writer, string target)
    {
        switch (writer)
        {
            case "csv":
                return File.ReadAllLines(target)[1];
            case "jsonl":
                var line = File.ReadAllLines(target)[0];
                return line[(line.IndexOf(':') + 1)..^1];
        }

        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        var from = writer == "duck" ? $"'{target}'.t" : $"read_parquet('{target}')";
        if (writer == "duck")
        {
            command.CommandText = $"ATTACH '{target}' AS f (READ_ONLY)";
            command.ExecuteNonQuery();
            from = "f.t";
        }
        command.CommandText = $"SET TimeZone = 'UTC'; SELECT typeof(c) || ' | ' || c::VARCHAR FROM {from}";
        return (string)command.ExecuteScalar()!;
    }
}
