using DuckDB.NET.Data;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters;

/// <summary>
/// The duck writer appending to an existing table refuses a value its column cannot hold, a decimal
/// with more digits or an integer or floating-point number out of range: exit 1, the column named, and
/// none of the rows it still buffers written. The refusal is the value's, not the type's: a source
/// wider than the target column is written while its values fit, and a value the column holds after a
/// conversion (a timestamp into a date) is written too.
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
    {
        var (stdout, stderr) = (Console.Out, Console.Error);
        var captured = new StringWriter();
        Console.SetOut(captured);
        Console.SetError(captured);
        try
        {
            var code = await Program.Main(["-i", "duck::memory:", "-q", query, "-o", $"duck:{target}", "-t", "t", "-s", "Append", "--no-stats"]);
            return (code, captured.ToString());
        }
        finally { Console.SetOut(stdout); Console.SetError(stderr); }
    }

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
