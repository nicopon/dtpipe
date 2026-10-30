using Apache.Arrow;
using AwesomeAssertions;
using DtPipe.Adapters.JsonL;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.JsonL;

/// <summary>
/// Without --column-types, a column reads into the type its sample gave, and a value that type does
/// not hold stops the read at its cell, naming the column, the type and where the cell stands, never
/// the value.
/// </summary>
public class JsonLInferredColumnTypesTests : IDisposable
{
    private readonly List<string> _paths = new();

    private string Source(params string[] lines)
    {
        var path = Path.Combine(Path.GetTempPath(), $"jsonl_inferred_{Guid.NewGuid()}.jsonl");
        _paths.Add(path);
        File.WriteAllLines(path, lines);
        return path;
    }

    public void Dispose()
    {
        foreach (var path in _paths) { try { File.Delete(path); } catch { /* best effort */ } }
    }

    private static JsonLReaderOptions Sample(int records, string? path = null)
        => new() { MaxSample = records, Path = path! };

    private static async Task<List<object?[]>> ReadRowsAsync(string source, JsonLReaderOptions options)
    {
        await using var reader = new JsonLStreamReader(source, options);
        await reader.OpenAsync();
        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(10)) rows.AddRange(batch.ToArray());
        return rows;
    }

    [Theory]
    [InlineData("1", "1.5", "int64", "1.5")]
    [InlineData("1", "true", "int64", "true")]
    [InlineData("1", "\"abc\"", "int64", "abc")]
    [InlineData("1", "\" \"", "int64", "\" \"")]
    [InlineData("1", "9223372036854775808", "int64", "9223372036854775808")]
    [InlineData("1", """{"a":1}""", "int64", "{")]
    [InlineData("1.5", "true", "double", "true")]
    [InlineData("1.5", "\"x\"", "double", "\"x\"")]
    [InlineData("true", "1", "bool", "1")]
    public async Task A_Value_The_Inferred_Type_Does_Not_Hold_Is_Refused_At_Its_Line(
        string sampled, string json, string hint, string quoted)
    {
        var source = Source($$"""{"id":1,"n":{{sampled}}}""", $$"""{"id":2,"n":{{sampled}}}""", $$"""{"id":3,"n":{{json}}}""");

        var read = () => ReadRowsAsync(source, Sample(2));

        var refusal = (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message;
        refusal.Should().Contain("JSONL line 3").And.Contain("'n'").And.Contain($"inferred as {hint}")
            .And.Contain("--max-sample").And.Contain("--column-types").And.NotContain(quoted);
    }

    public static TheoryData<string, string, object?> Readable => new()
    {
        { "1", "\"42\"", 42L },
        { "1", "2.0", 2L },
        { "1", "3000000000", 3000000000L },
        { "1", "\"\"", null },
        { "1", "null", null },
        { "1.5", "7", 7.0 },
        { "1.5", "\"2.5\"", 2.5 },
    };

    [Theory]
    [MemberData(nameof(Readable))]
    public async Task A_Value_The_Inferred_Type_Holds_Is_Read_Into_It(string sampled, string json, object? expected)
    {
        var source = Source($$"""{"id":1,"n":{{sampled}}}""", $$"""{"id":2,"n":{{json}}}""");

        var rows = await ReadRowsAsync(source, Sample(1));

        var value = rows[1][1];
        value.Should().Be(expected);
        value?.GetType().Should().Be(expected!.GetType());
    }

    [Fact]
    public async Task A_Text_Column_Keeps_A_Later_Number_As_Text()
    {
        var source = Source("""{"id":1,"n":"a"}""", """{"id":2,"n":5}""");

        var rows = await ReadRowsAsync(source, Sample(1));

        rows[1][1].Should().Be("5");
    }

    [Fact]
    public async Task A_Sampled_Record_The_Inferred_Type_Does_Not_Hold_Is_Refused_At_Its_Own_Line()
    {
        var source = Source("""{"id":1,"n":1}""", "", """{"id":2,"n":1.5}""");

        var read = () => ReadRowsAsync(source, Sample(2));

        (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("JSONL line 3").And.Contain("inferred as int64");
    }

    [Fact]
    public async Task The_Columnar_Path_Reads_The_Inferred_Type_And_Refuses_A_Fraction()
    {
        await using (var reader = new JsonLStreamReader(Source("""{"id":1,"n":1}""", """{"id":2,"n":"42"}""", """{"id":3,"n":2.0}"""), Sample(1)))
        {
            await reader.OpenAsync();
            var values = new List<long?>();
            await foreach (var batch in reader.ReadRecordBatchesAsync())
            {
                using (batch) values.AddRange(((Int64Array)batch.Column("n")).ToList());
            }
            values.Should().Equal(1, 42, 2);
        }

        await using (var reader = new JsonLStreamReader(Source("""{"id":1,"n":1}""", """{"id":2,"n":1.5}"""), Sample(1)))
        {
            await reader.OpenAsync();
            var read = async () => { await foreach (var batch in reader.ReadRecordBatchesAsync()) batch.Dispose(); };
            (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
                .Should().Contain("JSONL line 2").And.Contain("inferred as int64");
        }
    }

    [Fact]
    public async Task Under_A_Path_The_Refusal_Names_The_Record()
    {
        var source = Source("""{"items":[{"id":1,"n":1},{"id":2,"n":2},{"id":3,"n":1.5}]}""");
        await using var reader = new JsonLStreamReader(source, Sample(2, "items"));
        await reader.OpenAsync();

        var read = async () => { await foreach (var batch in reader.ReadRecordBatchesAsync()) batch.Dispose(); };

        (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("JSONL record 3").And.Contain("'n'").And.Contain("inferred as int64");
    }
}
