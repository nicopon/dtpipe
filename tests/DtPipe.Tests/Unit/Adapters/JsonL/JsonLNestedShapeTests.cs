using Apache.Arrow;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Adapters.JsonL;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.JsonL;

/// <summary>
/// An object column whose keys vary from one record to the next reaches the destination whole, never
/// with a key missing and never with a value that belongs to another record.
/// </summary>
public class JsonLNestedShapeTests : IDisposable
{
    private readonly List<string> _paths = new();

    private string TempFile(string extension, IEnumerable<string>? lines = null)
    {
        var path = Path.Combine(Path.GetTempPath(), $"jsonl_shape_{Guid.NewGuid()}{extension}");
        _paths.Add(path);
        if (lines != null) File.WriteAllLines(path, lines);
        return path;
    }

    public void Dispose()
    {
        foreach (var path in _paths) { try { File.Delete(path); } catch { /* best effort */ } }
    }

    [Fact]
    public async Task Objects_Of_Varying_Shape_Are_Written_Key_For_Key()
    {
        var events = JsonLShapeData.Events(120).ToList();
        var source = TempFile(".jsonl", events);
        var target = TempFile(".jsonl");

        var exit = await Program.Main(["-i", $"jsonl:{source}", "-o", $"jsonl:{target}", "--no-stats"]);

        exit.Should().Be(0);
        JsonLShapeData.Differences(events, JsonLShapeData.PayloadsOfJsonl(target)).Should().BeEmpty();
    }

    [Fact]
    public async Task A_Sample_Larger_Than_One_Gives_No_Row_The_Values_Of_Another()
    {
        var events = new[]
        {
            """{"id":0,"payload":{"severity":"info"}}""",
            """{"id":1,"payload":{"severity":"warn","error_code":"E1"}}"""
        };
        var source = TempFile(".jsonl", events);
        var target = TempFile(".jsonl");

        var exit = await Program.Main(["-i", $"jsonl:{source}", "--max-sample", "2", "-o", $"jsonl:{target}", "--no-stats"]);

        exit.Should().Be(0);
        JsonLShapeData.Differences(events, JsonLShapeData.PayloadsOfJsonl(target)).Should().BeEmpty();
    }

    [Fact]
    public async Task A_Max_Sample_Below_The_Target_Arrays_Size_Still_Yields_Every_Element()
    {
        var source = TempFile(".jsonl", new[]
        {
            """{"items":[{"id":1,"v":"a"},{"id":2,"v":"b"},{"id":3,"v":"c"},{"id":4,"v":"d"},{"id":5,"v":"e"}]}"""
        });

        await using var reader = new JsonLStreamReader(source, new JsonLReaderOptions { Path = "items", MaxSample = 2 });
        await reader.OpenAsync();

        var total = 0;
        await foreach (var batch in reader.ReadRecordBatchesAsync())
        {
            total += batch.Length;
            batch.Dispose();
        }

        total.Should().Be(5);
    }

    [Fact]
    public async Task A_Value_Gets_The_Same_Text_Whether_The_Sample_Saw_It_Or_Not()
    {
        var events = new[]
        {
            """{"id":1,"col":"abc"}""",
            """{"id":2,"col":42}""",
            """{"id":3,"col":99}""",
            """{"id":4,"col":"z"}"""
        };
        var source = TempFile(".jsonl", events);
        var target = TempFile(".jsonl");

        // max-sample 2 buffers the first two records (where 'col' is inferred string from
        // record 1, then record 2's number must already resolve to text); records 3 and 4
        // come from the fresh read that follows.
        var exit = await Program.Main(["-i", $"jsonl:{source}", "--max-sample", "2", "-o", $"jsonl:{target}", "--no-stats"]);

        exit.Should().Be(0);
        var lines = await File.ReadAllLinesAsync(target);
        lines.Should().HaveCount(4);

        var values = lines.Select(l =>
        {
            using var doc = System.Text.Json.JsonDocument.Parse(l);
            return doc.RootElement.GetProperty("col").GetString();
        }).ToList();
        values.Should().Equal("abc", "42", "99", "z");
    }

    [Fact]
    public async Task Sample_Zero_Turns_Inference_Off_And_Asks_For_A_Declared_Schema()
    {
        var source = TempFile(".jsonl", JsonLShapeData.Events(3));

        await using (var reader = new JsonLStreamReader(source, new JsonLReaderOptions { MaxSample = 0 }))
        {
            var refused = async () => await reader.OpenAsync();
            (await refused.Should().ThrowAsync<InvalidOperationException>())
                .WithMessage("*--max-sample 0*--schema-load*--column-types*");
        }

        // Columns the caller names need no sample.
        await using var declared = new JsonLStreamReader(source, new JsonLReaderOptions { MaxSample = 0, ColumnTypes = "id:int64" });
        await declared.OpenAsync();
        declared.Columns!.Select(c => c.Name).Should().Equal("id");
    }

    [Fact]
    public async Task A_Column_Types_Hint_Naming_No_Type_Is_Refused_Not_Defaulted_To_String()
    {
        var source = TempFile(".jsonl", new[] { """{"id":1,"col":"x"}""" });

        await using var reader = new JsonLStreamReader(source, new JsonLReaderOptions { ColumnTypes = "id:int64,col:jsno" });
        var refused = async () => await reader.OpenAsync();
        (await refused.Should().ThrowAsync<InvalidOperationException>())
            .WithMessage("*'col'*'jsno'*names no type*");
    }

    [Fact]
    public async Task A_Declared_Schema_Refuses_A_Key_It_Lacks_Too()
    {
        var declared = new Schema(new[]
        {
            new Field("id", Int64Type.Default, true),
            new Field("payload", new StructType(new[] { new Field("severity", StringType.Default, true) }), true)
        }, null);
        var source = TempFile(".jsonl", new[]
        {
            """{"id":0,"payload":{"severity":"info"}}""",
            """{"id":1,"payload":{"severity":"info","extra":"x"}}"""
        });

        await using var reader = new JsonLStreamReader(source, new JsonLReaderOptions { Schema = ArrowSchemaSerializer.SerializeCompact(declared) });
        await reader.OpenAsync();

        var refused = async () => { await foreach (var batch in reader.ReadRecordBatchesAsync()) batch.Dispose(); };
        (await refused.Should().ThrowAsync<InvalidOperationException>())
            .WithMessage("*record 2*'payload.extra'*declared schema*");
    }

    [Fact]
    public async Task A_Key_Beyond_The_Sample_Is_Refused_By_Name_Never_Dropped()
    {
        var events = JsonLShapeData.Events(1000)
            .Append("""{"id":1000,"payload":{"severity":"info","late_key":"x"}}""").ToList();
        var source = TempFile(".jsonl", events);

        // The default sample is 1000 records: the 1001st is the first one past it.
        var refused = async () => await ReadAll(new JsonLReaderOptions());
        (await refused.Should().ThrowAsync<InvalidOperationException>())
            .WithMessage("*record 1001*'payload.late_key'*--max-sample*");

        // The escape the message names: a sample that reaches it.
        var rows = await ReadAll(new JsonLReaderOptions { MaxSample = 1001 });
        rows.Should().Be(1001);

        async Task<int> ReadAll(JsonLReaderOptions options)
        {
            await using var reader = new JsonLStreamReader(source, options);
            await reader.OpenAsync();
            var total = 0;
            await foreach (var batch in reader.ReadRecordBatchesAsync())
            {
                total += batch.Length;
                batch.Dispose();
            }
            return total;
        }
    }

    [Fact]
    public async Task Declaring_An_Object_Column_As_String_Keeps_Raw_Json_Text()
    {
        var events = new[]
        {
            """{"id":1,"payload":{"severity":"info"}}""",
            """{"id":2,"payload":{"severity":"warn","error_code":"E1"}}"""
        };
        var source = TempFile(".jsonl", events);
        var target = TempFile(".jsonl");

        var exit = await Program.Main(["-i", $"jsonl:{source}", "--column-types", "id:int64,payload:string", "-o", $"jsonl:{target}", "--no-stats"]);

        exit.Should().Be(0);
        var outputLines = await File.ReadAllLinesAsync(target);
        outputLines.Should().HaveCount(2);

        using (var doc0 = System.Text.Json.JsonDocument.Parse(outputLines[0]))
        {
            doc0.RootElement.GetProperty("id").GetInt64().Should().Be(1);
            doc0.RootElement.GetProperty("payload").GetString().Should().Be("""{"severity":"info"}""");
        }

        using (var doc1 = System.Text.Json.JsonDocument.Parse(outputLines[1]))
        {
            doc1.RootElement.GetProperty("id").GetInt64().Should().Be(2);
            doc1.RootElement.GetProperty("payload").GetString().Should().Be("""{"severity":"warn","error_code":"E1"}""");
        }
    }

    [Fact]
    public async Task A_String_Column_Keeps_The_Raw_Json_Text_Of_A_Scalar_Too()
    {
        var source = TempFile(".jsonl", new[] { """{"id":1,"f":true,"n":1.50}""" });
        var target = TempFile(".jsonl");

        var exit = await Program.Main(["-i", $"jsonl:{source}", "--column-types", "id:string,f:string,n:string", "-o", $"jsonl:{target}", "--no-stats"]);

        exit.Should().Be(0);
        var line = (await File.ReadAllLinesAsync(target)).Single();
        using var doc = System.Text.Json.JsonDocument.Parse(line);
        doc.RootElement.GetProperty("id").GetString().Should().Be("1");
        doc.RootElement.GetProperty("f").GetString().Should().Be("true");
        doc.RootElement.GetProperty("n").GetString().Should().Be("1.50");
    }

    [Fact]
    public async Task Reader_Direct_ReadRecordBatchesAsync_With_ColumnTypes_String_On_Object()
    {
        var events = new[]
        {
            """{"id":1,"payload":{"severity":"info"}}"""
        };
        var source = TempFile(".jsonl", events);
        await using var reader = new JsonLStreamReader(source, new JsonLReaderOptions { ColumnTypes = "id:int64,payload:string" });
        await reader.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadRecordBatchesAsync())
        {
            batches.Add(batch);
        }

        batches.Should().HaveCount(1);
        var array = batches[0].Column("payload").Should().BeOfType<StringArray>().Subject;
        array.GetString(0).Should().Be("""{"severity":"info"}""");
        foreach (var b in batches) b.Dispose();
    }

    [Fact]
    public async Task Reader_Direct_ReadBatchesAsync_With_ColumnTypes_String_On_Object()
    {
        var events = new[]
        {
            """{"id":1,"payload":{"severity":"info"}}"""
        };
        var source = TempFile(".jsonl", events);
        await using var reader = new JsonLStreamReader(source, new JsonLReaderOptions { ColumnTypes = "id:int64,payload:string" });
        await reader.OpenAsync();
        var rows = new List<object?[]>();
        await foreach (var memory in reader.ReadBatchesAsync(10))
        {
            for (int i = 0; i < memory.Length; i++) rows.Add(memory.Span[i]);
        }

        rows.Should().HaveCount(1);
        rows[0][1].Should().Be("""{"severity":"info"}""");
    }

    [Fact]
    public async Task Object_Column_Edge_Cases_Null_Empty_And_Nested()
    {
        var events = new[]
        {
            """{"id":1,"payload":{"nested":{"a":[1,2],"b":"xyz"}}}""",
            """{"id":2,"payload":null}""",
            """{"id":3,"payload":{}}""",
            """{"id":4,"payload":""}""",
            """{"id":5,"payload":[{"k":1}]}"""
        };
        var source = TempFile(".jsonl", events);
        var target = TempFile(".jsonl");

        var exit = await Program.Main(["-i", $"jsonl:{source}", "--column-types", "id:int64,payload:string", "-o", $"jsonl:{target}", "--no-stats"]);

        exit.Should().Be(0);
        var lines = await File.ReadAllLinesAsync(target);
        lines.Should().HaveCount(5);

        using (var d1 = System.Text.Json.JsonDocument.Parse(lines[0]))
            d1.RootElement.GetProperty("payload").GetString().Should().Be("""{"nested":{"a":[1,2],"b":"xyz"}}""");

        using (var d2 = System.Text.Json.JsonDocument.Parse(lines[1]))
            d2.RootElement.GetProperty("payload").ValueKind.Should().Be(System.Text.Json.JsonValueKind.Null);

        using (var d3 = System.Text.Json.JsonDocument.Parse(lines[2]))
            d3.RootElement.GetProperty("payload").GetString().Should().Be("{}");

        using (var d4 = System.Text.Json.JsonDocument.Parse(lines[3]))
            d4.RootElement.GetProperty("payload").GetString().Should().Be("");

        using (var d5 = System.Text.Json.JsonDocument.Parse(lines[4]))
            d5.RootElement.GetProperty("payload").GetString().Should().Be("""[{"k":1}]""");
    }

    [Fact]
    public async Task Declared_Schema_With_StringType_Keeps_Raw_Json_Text()
    {
        var declared = new Schema(new[]
        {
            new Field("id", Int64Type.Default, true),
            new Field("payload", StringType.Default, true)
        }, null);
        var events = new[]
        {
            """{"id":1,"payload":{"severity":"info"}}""",
            """{"id":2,"payload":{"severity":"warn"}}"""
        };
        var source = TempFile(".jsonl", events);
        await using var reader = new JsonLStreamReader(source, new JsonLReaderOptions
        {
            Schema = ArrowSchemaSerializer.SerializeCompact(declared)
        });
        await reader.OpenAsync();

        var batches = new List<RecordBatch>();
        await foreach (var batch in reader.ReadRecordBatchesAsync()) batches.Add(batch);

        batches.Should().HaveCount(1);
        var array = batches[0].Column("payload").Should().BeOfType<StringArray>().Subject;
        array.GetString(0).Should().Be("""{"severity":"info"}""");
        array.GetString(1).Should().Be("""{"severity":"warn"}""");
        foreach (var b in batches) b.Dispose();
    }
}
