using Apache.Arrow;
using AwesomeAssertions;
using DtPipe.Adapters.JsonL;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.JsonL;

/// <summary>
/// Without --column-types, the fields of a struct column and the items of a list column read into the
/// type the sample gave them, as a top-level column does: a value that type does not hold stops the
/// read at its record, naming the path of the field, never the value.
/// </summary>
public class JsonLInferredNestedTypesTests : IDisposable
{
    private readonly List<string> _paths = new();

    private string Source(params string[] lines)
    {
        var path = Path.Combine(Path.GetTempPath(), $"jsonl_nested_{Guid.NewGuid()}.jsonl");
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

    // Two sampled records give the column its shape; the third carries the value under test.
    private string Column(string column, string sampled, string value)
        => Source($$"""{"id":1,"{{column}}":{{sampled}}}""", $$"""{"id":2,"{{column}}":{{sampled}}}""", $$"""{"id":3,"{{column}}":{{value}}}""");

    [Theory]
    [InlineData("s", """{"a":1}""", """{"a":1.5}""", "s.a", "int64", "1.5")]
    [InlineData("s", """{"a":1}""", """{"a":true}""", "s.a", "int64", "true")]
    [InlineData("s", """{"a":1}""", """{"a":"abc"}""", "s.a", "int64", "abc")]
    [InlineData("s", """{"a":1}""", """{"a":{"b":1}}""", "s.a", "int64", "{")]
    [InlineData("s", """{"a":1.5}""", """{"a":"x"}""", "s.a", "double", "\"x\"")]
    [InlineData("s", """{"a":true}""", """{"a":1}""", "s.a", "bool", "1")]
    [InlineData("s", """{"a":{"b":1}}""", """{"a":{"b":1.5}}""", "s.a.b", "int64", "1.5")]
    [InlineData("l", "[1,2]", "[1.5]", "l[]", "int64", "1.5")]
    [InlineData("l", "[1,2]", "[1,\"abc\"]", "l[]", "int64", "abc")]
    [InlineData("l", """[{"a":1}]""", """[{"a":1},{"a":1.5}]""", "l[].a", "int64", "1.5")]
    [InlineData("s", """{"c":[1]}""", """{"c":[true]}""", "s.c[]", "int64", "true")]
    public async Task A_Nested_Value_The_Inferred_Type_Does_Not_Hold_Is_Refused_At_Its_Line(
        string column, string sampled, string value, string path, string hint, string quoted)
    {
        var source = Column(column, sampled, value);

        var read = () => ReadRowsAsync(source, Sample(2));

        var refusal = (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message;
        refusal.Should().Contain("JSONL line 3").And.Contain($"'{path}'").And.Contain($"inferred as {hint}")
            .And.Contain($"column '{column}'").And.Contain("--max-sample").And.Contain("--column-types")
            .And.NotContain(quoted);
    }

    public static TheoryData<string, string, string, string> Readable => new()
    {
        { "s", """{"a":1}""", """{"a":"42"}""", """{"a":42}""" },
        { "s", """{"a":1}""", """{"a":2.0}""", """{"a":2}""" },
        { "s", """{"a":1}""", """{"a":null}""", """{"a":null}""" },
        { "s", """{"a":1}""", """{"a":""}""", """{"a":null}""" },
        { "s", """{"a":1}""", "{}", "{}" },
        { "s", """{"a":1}""", "null", "null" },
        { "s", """{"a":1.5}""", """{"a":7}""", """{"a":7}""" },
        { "s", """{"a":"t"}""", """{"a":5}""", """{"a":5}""" },
        { "s", """{"a":{"b":1}}""", """{"a":{"b":"3"}}""", """{"a":{"b":3}}""" },
        { "l", "[1,2]", """["7",8.0]""", "[7,8]" },
        { "l", "[1,2]", "[]", "[]" },
        { "l", "[1,2]", "[null]", "[null]" },
        { "l", """[{"a":1}]""", """[{"a":"5"},{}]""", """[{"a":5},{}]""" },
    };

    [Theory]
    [MemberData(nameof(Readable))]
    public async Task A_Nested_Value_The_Inferred_Type_Holds_Is_Read_Into_It(
        string column, string sampled, string value, string expected)
    {
        var rows = await ReadRowsAsync(Column(column, sampled, value), Sample(1));

        Render(rows[2][1]).Should().Be(expected);
    }

    [Fact]
    public async Task A_Key_Outside_The_Schema_Is_Still_Named_By_Its_Own_Refusal()
    {
        var read = () => ReadRowsAsync(Column("s", """{"a":1}""", """{"a":2,"b":2}"""), Sample(2));

        (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("key 's.b'");
    }

    [Fact]
    public async Task A_Sampled_Record_The_Inferred_Type_Does_Not_Hold_Is_Refused_At_Its_Own_Line()
    {
        var source = Source("""{"id":1,"s":{"a":1}}""", "", """{"id":2,"s":{"a":1.5}}""");

        var read = () => ReadRowsAsync(source, Sample(2));

        (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("JSONL line 3").And.Contain("'s.a'");
    }

    [Fact]
    public async Task The_Columnar_Path_Reads_The_Inferred_Field_Type_And_Refuses_A_Fraction()
    {
        await using (var reader = new JsonLStreamReader(Column("s", """{"a":1}""", """{"a":"42"}"""), Sample(1)))
        {
            await reader.OpenAsync();
            var values = new List<long?>();
            await foreach (var batch in reader.ReadRecordBatchesAsync())
            {
                using (batch) values.AddRange(((Int64Array)((StructArray)batch.Column("s")).Fields[0]).ToList());
            }
            values.Should().Equal(1, 1, 42);
        }

        await using (var reader = new JsonLStreamReader(Column("l", "[1]", "[1.5]"), Sample(1)))
        {
            await reader.OpenAsync();
            var read = async () => { await foreach (var batch in reader.ReadRecordBatchesAsync()) batch.Dispose(); };
            (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
                .Should().Contain("JSONL line 3").And.Contain("'l[]'").And.Contain("inferred as int64");
        }
    }

    [Fact]
    public async Task Under_A_Path_The_Refusal_Names_The_Record_And_The_Field()
    {
        var source = Source("""{"items":[{"id":1,"s":{"a":1}},{"id":2,"s":{"a":2}},{"id":3,"s":{"a":1.5}}]}""");
        await using var reader = new JsonLStreamReader(source, Sample(2, "items"));
        await reader.OpenAsync();

        var read = async () => { await foreach (var batch in reader.ReadRecordBatchesAsync()) batch.Dispose(); };

        (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("JSONL record 3").And.Contain("'s.a'").And.Contain("inferred as int64");

        var rows = () => ReadRowsAsync(source, Sample(2, "items"));
        (await rows.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("JSONL record 3").And.Contain("'s.a'");
    }

    // A struct renders as {k:v,...}, a list as [v,...] and a text in quotes, so one string states the
    // whole value and its types.
    private static string Render(object? value) => value switch
    {
        null => "null",
        string s => $"\"{s}\"",
        Dictionary<string, object?> d => "{" + string.Join(",", d.Select(p => $"\"{p.Key}\":{Render(p.Value)}")) + "}",
        System.Collections.IEnumerable l when value is not string => "[" + string.Join(",", l.Cast<object?>().Select(Render)) + "]",
        _ => value.ToString()!,
    };
}
