using System.Text.RegularExpressions;
using DuckDB.NET.Data;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters;

/// <summary>
/// The duck writer appending to an existing table refuses a value its column cannot hold, a decimal
/// with more digits or an integer or floating-point number out of range, and a value the writer does
/// not bring to its column's type: exit 1, the column named, and none of the rows it still buffers
/// written. A source wider than the target column is written while its values fit, and a value the
/// column holds after a conversion (a timestamp into a date, a text into a BLOB) is written too.
/// </summary>
[Collection("console-serial")]
public sealed class DuckDbWriterValueOverflowTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("dtpipe-duck-overflow-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public async Task A_Decimal_With_More_Digits_Than_The_Target_Column_Is_Refused_Naming_The_Column()
    {
        var target = CreateTarget("amount", "DECIMAL(5,2)", "5.5");

        var (code, output) = await AppendAsync(target,
            "select * from (values (2, 1.5::decimal(10,2)), (3, 123456.78::decimal(10,2))) v(id, amount)");

        // Neither row of the failed run is written: not the one that fits, nor the one that does not.
        Assert.Equal(["1 | 5.50"], ReadBack(target, "amount"));
        Assert.Equal(1, code);
        Assert.Contains("Column 'amount'", output);
    }

    [Fact]
    public async Task A_Wider_Decimal_Whose_Values_Fit_The_Target_Column_Is_Written()
    {
        var target = CreateTarget("amount", "DECIMAL(5,2)", "5.5");

        var (code, _) = await AppendAsync(target, "select 2 as id, 999.99::decimal(10,2) as amount");

        Assert.Equal(0, code);
        Assert.Equal(["1 | 5.50", "2 | 999.99"], ReadBack(target, "amount"));
    }

    [Theory]
    [InlineData("BIGINT", "INTEGER", "5", "3000000000")]
    [InlineData("INTEGER", "SMALLINT", "5", "40000")]
    [InlineData("INTEGER", "UTINYINT", "5", "-1")]
    [InlineData("DOUBLE", "INTEGER", "5", "3e9")]
    public async Task A_Number_Out_Of_Range_For_The_Target_Column_Is_Refused_Naming_The_Column(
        string sourceType, string targetType, string fits, string overflows)
    {
        var target = CreateTarget("n", targetType, "10");

        var (code, output) = await AppendAsync(target,
            $"select * from (values (2, {fits}::{sourceType}), (3, {overflows}::{sourceType})) v(id, n)");

        // Neither row of the failed run is written: not the one that fits, nor the one that does not.
        Assert.Equal(["1 | 10"], ReadBack(target, "n"));
        Assert.Equal(1, code);
        Assert.Contains("Column 'n' cannot hold a value of the source.", output);
    }

    [Fact]
    public async Task A_Wider_Integer_Whose_Values_Fit_The_Target_Column_Is_Written()
    {
        var target = CreateTarget("n", "INTEGER", "10");

        var (code, _) = await AppendAsync(target, "select 2 as id, 5::BIGINT as n");

        Assert.Equal(0, code);
        Assert.Equal(["1 | 10", "2 | 5"], ReadBack(target, "n"));
    }

    [Fact]
    public async Task A_Timestamp_Is_Written_To_A_Date_Column()
    {
        var target = CreateTarget("n", "DATE", "DATE '2024-01-01'");

        var (code, _) = await AppendAsync(target, "select 2 as id, TIMESTAMP '2024-01-02 00:00:00' as n");

        Assert.Equal(0, code);
        Assert.Equal(["1 | 2024-01-01", "2 | 2024-01-02"], ReadBack(target, "n"));
    }

    // Values that fit their column, of a type the writer does not convert to the column's: the
    // appender would store their bytes at the width of their own type.
    [Theory]
    [InlineData("HUGEINT", "BIGINT", "10", "10", "1200", "1225", "1250")]
    [InlineData("DATE", "TIMESTAMP", "TIMESTAMP '2024-01-01 00:00:00'", "2024-01-01 00:00:00", "'2024-01-02'", "'2024-01-03'", "'2024-01-04'")]
    [InlineData("INTEGER", "HUGEINT", "10", "10", "5", "6", "7")]
    [InlineData("TIMESTAMP", "TIME", "TIME '01:00:00'", "01:00:00", "'2024-01-02 13:45:00'", "'2024-01-03 13:45:00'", "'2024-01-04 13:45:00'")]
    public async Task A_Value_Of_Another_Type_Than_Its_Column_Is_Refused_Naming_The_Column(
        string sourceType, string targetType, string first, string firstRead, string a, string b, string c)
    {
        var target = CreateTarget("n", targetType, first);

        var (code, output) = await AppendAsync(target,
            $"select * from (values (2, CAST({a} AS {sourceType})), (3, CAST({b} AS {sourceType})), (4, CAST({c} AS {sourceType}))) v(id, n)");

        // None of the three rows is written.
        Assert.Equal([$"1 | {firstRead}"], ReadBack(target, "n"));
        Assert.Equal(1, code);
        var message = Regex.Replace(output, @"\s+", " ");
        Assert.Contains($"Column 'n' is {targetType} and the source sends {sourceType}", message);
        Assert.Contains($"CAST(n AS {targetType})", message);
    }

    [Fact]
    public async Task A_Text_Is_Written_To_A_Blob_Column()
    {
        var target = CreateTarget("n", "BLOB", "'z'::BLOB");

        var (code, _) = await AppendAsync(target, "select 2 as id, 'ab' as n");

        Assert.Equal(0, code);
        Assert.Equal(["1 | z", "2 | ab"], ReadBack(target, "n"));
    }

    [Fact]
    public async Task A_Narrower_Integer_Is_Written_To_A_Wider_Column()
    {
        var target = CreateTarget("n", "BIGINT", "10");

        var (code, _) = await AppendAsync(target, "select 2 as id, 5::INTEGER as n");

        Assert.Equal(0, code);
        Assert.Equal(["1 | 10", "2 | 5"], ReadBack(target, "n"));
    }

    // The writer reads a column of a type it does not know as text, and leaves it unchecked.
    [Fact]
    public async Task A_Text_Is_Written_To_An_Enum_Column()
    {
        var target = CreateTarget("n", "ENUM('a', 'b', 'c')", "'c'");

        var (code, _) = await AppendAsync(target, "select * from (values (2, 'a'), (3, 'b')) v(id, n)");

        Assert.Equal(0, code);
        Assert.Equal(["1 | c", "2 | a", "3 | b"], ReadBack(target, "n"));
    }

    private string CreateTarget(string column, string columnType, string firstValue)
    {
        var target = Path.Combine(_dir, Guid.NewGuid().ToString("N") + ".duckdb");
        using var connection = new DuckDBConnection($"Data Source={target}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"CREATE TABLE t (id INTEGER, {column} {columnType}); INSERT INTO t VALUES (1, {firstValue})";
        command.ExecuteNonQuery();
        return target;
    }

    private static async Task<(int Code, string Output)> AppendAsync(string target, string query)
        => await ConsoleSerialLock.CaptureAsync(() =>
            Program.Main(["-i", "duck::memory:", "-q", query, "-o", $"duck:{target}", "-t", "t", "-s", "Append", "--no-stats"]));

    private static List<string> ReadBack(string target, string column)
    {
        using var connection = new DuckDBConnection($"Data Source={target}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT id::VARCHAR || ' | ' || {column}::VARCHAR FROM t ORDER BY id";
        using var reader = command.ExecuteReader();
        var rows = new List<string>();
        while (reader.Read()) rows.Add(reader.GetString(0));
        return rows;
    }
}
