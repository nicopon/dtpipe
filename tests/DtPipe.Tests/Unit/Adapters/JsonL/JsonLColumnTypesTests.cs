using Apache.Arrow;
using AwesomeAssertions;
using DtPipe.Adapters.JsonL;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.JsonL;

/// <summary>
/// A column --column-types declares reads into its type without loss: a value the type does not hold
/// stops the read at its cell, naming the column, the type and where the cell stands, never the value.
/// </summary>
public class JsonLColumnTypesTests : IDisposable
{
    private readonly List<string> _paths = new();

    private string Source(params string[] lines)
    {
        var path = Path.Combine(Path.GetTempPath(), $"jsonl_types_{Guid.NewGuid()}.jsonl");
        _paths.Add(path);
        File.WriteAllLines(path, lines);
        return path;
    }

    public void Dispose()
    {
        foreach (var path in _paths) { try { File.Delete(path); } catch { /* best effort */ } }
    }

    private static async Task<List<object?[]>> ReadRowsAsync(string source, JsonLReaderOptions options)
    {
        await using var reader = new JsonLStreamReader(source, options);
        await reader.OpenAsync();
        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(10)) rows.AddRange(batch.ToArray());
        return rows;
    }

    [Theory]
    [InlineData("int", "1.5", "int32", "1.5")]
    [InlineData("int", "true", "int32", "true")]
    [InlineData("int", "\"abc\"", "int32", "abc")]
    [InlineData("int", "\" \"", "int32", "\" \"")]
    [InlineData("int", "3000000000", "int32", "3000000000")]
    [InlineData("int", """{"a":1}""", "int32", "{")]
    [InlineData("double", "\"1e400\"", "double", "1e400")]
    public async Task A_Value_The_Declared_Type_Does_Not_Hold_Is_Refused_At_Its_Line(
        string type, string json, string hint, string quoted)
    {
        var source = Source("""{"id":1,"n":null}""", $$"""{"id":2,"n":{{json}}}""");

        var read = () => ReadRowsAsync(source, new JsonLReaderOptions { ColumnTypes = $"id:int64,n:{type}" });

        var refusal = (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message;
        refusal.Should().Contain("JSONL line 2").And.Contain("'n'").And.Contain($"declared {hint}").And.NotContain(quoted);
    }

    public static TheoryData<string, string, object?> Readable => new()
    {
        { "int", "2.0", 2 },
        { "int", "\"42\"", 42 },
        { "int", "\"\"", null },
        { "double", "\"Infinity\"", double.PositiveInfinity },
        { "bool", "\"yes\"", true },
        { "datetime", "\"2024-01-02\"", new DateTime(2024, 1, 2) },
        { "datetimeoffset", "\"2024-01-02T00:00:00Z\"", new DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.Zero) },
        { "uuid", "\"6f9619ff-8b86-d011-b42d-00c04fc964ff\"", Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff") },
    };

    [Theory]
    [MemberData(nameof(Readable))]
    public async Task A_Value_The_Declared_Type_Holds_Is_Read_Into_It(string type, string json, object? expected)
    {
        var source = Source($$"""{"id":1,"n":{{json}}}""");

        var rows = await ReadRowsAsync(source, new JsonLReaderOptions { ColumnTypes = $"id:int64,n:{type}" });

        var value = rows.Should().ContainSingle().Which[1];
        value.Should().Be(expected);
        value?.GetType().Should().Be(expected!.GetType());
    }

    [Fact]
    public async Task The_Columnar_Path_Reads_The_Declared_Type_And_Refuses_A_Fraction()
    {
        var options = new JsonLReaderOptions { ColumnTypes = "id:int64,n:int" };

        await using (var reader = new JsonLStreamReader(Source("""{"id":1,"n":"42"}""", """{"id":2,"n":2.0}"""), options))
        {
            await reader.OpenAsync();
            var values = new List<int?>();
            await foreach (var batch in reader.ReadRecordBatchesAsync())
            {
                using (batch) values.AddRange(((Int32Array)batch.Column("n")).ToList());
            }
            values.Should().Equal(42, 2);
        }

        await using (var reader = new JsonLStreamReader(Source("""{"id":1,"n":2}""", "", """{"id":3,"n":1.5}"""), options))
        {
            await reader.OpenAsync();
            var read = async () => { await foreach (var batch in reader.ReadRecordBatchesAsync()) batch.Dispose(); };
            (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
                .Should().Contain("JSONL line 3").And.Contain("declared int32");
        }
    }

    [Fact]
    public async Task Under_A_Path_The_Refusal_Names_The_Record()
    {
        var source = Source("""{"items":[{"id":1,"n":2},{"id":2,"n":1.5}]}""");
        await using var reader = new JsonLStreamReader(source, new JsonLReaderOptions { Path = "items", ColumnTypes = "id:int64,n:int" });
        await reader.OpenAsync();

        var read = async () => { await foreach (var batch in reader.ReadRecordBatchesAsync()) batch.Dispose(); };

        (await read.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("JSONL record 2").And.Contain("'n'").And.Contain("declared int32");
    }
}
