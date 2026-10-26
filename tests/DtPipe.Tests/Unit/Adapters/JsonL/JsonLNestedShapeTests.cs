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
}
