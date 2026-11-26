using DtPipe.Core.Models;
using DtPipe.Transformers.Services;
using DtPipe.Transformers.Row.Window;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

public class WindowDataTransformerTests : IDisposable
{
    private readonly JsEngineProvider _jsProvider;

    public WindowDataTransformerTests()
    {
        _jsProvider = new JsEngineProvider();
    }

    public void Dispose()
    {
        _jsProvider.Dispose();
    }

    [Fact]
    public async Task Initialize_ShouldCompileScript()
    {
        // Arrange
        var options = new WindowOptions { Script = "return rows;" };
        var transformer = new WindowDataTransformer(options, _jsProvider);
        var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false) };

        // Act
        var result = await transformer.InitializeAsync(columns);

        // Assert
        result.Should().BeEquivalentTo(columns);
    }

    [Fact]
    public async Task ProcessRow_CountStrategy_ShouldBatchAndFlush()
    {
        // Arrange
        var options = new WindowOptions
        {
            Count = 2,
            Script = "return [ { Id: rows.length } ];"
        };
        var transformer = new WindowDataTransformer(options, _jsProvider);
        var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        await transformer.InitializeAsync(columns);

        // Act & Assert

        // Row 1: Buffered
        var res1 = transformer.TransformMany(new object?[] { 1 }).ToList();
        res1.Should().BeEmpty();

        // Row 2: Trigger Window (Size 2)
        var res2 = transformer.TransformMany(new object?[] { 2 }).ToList();
        res2.Should().HaveCount(1);
        res2.First()[0].Should().Be(2d); // Jint number

        // Row 3: Buffered
        var res3 = transformer.TransformMany(new object?[] { 3 }).ToList();
        res3.Should().BeEmpty();

        // Flush remaining (Row 3)
        var resFlush = transformer.Flush().ToList();
        resFlush.Should().HaveCount(1);
        resFlush.First()[0].Should().Be(1d);
    }

    [Fact]
    public async Task ProcessRow_KeyStrategy_ShouldFlushOnKeyChange()
    {
        // Arrange
        var options = new WindowOptions
        {
            Key = "Category",
            Script = "return [ { Category: rows[0].Category, Val: rows.reduce((a,b) => a + b.Val, 0) } ];"
        };
        var transformer = new WindowDataTransformer(options, _jsProvider);
        var columns = new List<PipeColumnInfo>
        {
            new("Category", typeof(string), false),
            new("Val", typeof(int), false)
        };
        await transformer.InitializeAsync(columns);

        // Act & Assert

        // Row 1: A, 10 -> Buffered (Key A)
        var r1 = transformer.TransformMany(new object?[2] { "A", 10 }).ToList();
        r1.Should().BeEmpty();

        // Row 2: A, 20 -> Buffered (Key A matches)
        var r2 = transformer.TransformMany(new object?[2] { "A", 20 }).ToList();
        r2.Should().BeEmpty();

        // Row 3: B, 5 -> Key Change! Flush A (Sum 30). Buffer B.
        var r3 = transformer.TransformMany(new object?[2] { "B", 5 }).ToList();
        r3.Should().HaveCount(1);
        r3.First()[0].Should().Be("A");
        r3.First()[1].Should().Be(30d);

        // Flush (Flush B, Sum 5)
        var rFlush = transformer.Flush().ToList();
        rFlush.Should().HaveCount(1);
        rFlush.First()[0].Should().Be("B");
        rFlush.First()[1].Should().Be(5d);
    }
    [Fact]
    public async Task ProcessRow_NoStrategy_ShouldBufferAllAndFlush()
    {
        // Arrange
        // No Count, No Key = Implicit "Window All"
        var options = new WindowOptions
        {
            Script = "return [ { Id: rows.length } ];"
        };
        var transformer = new WindowDataTransformer(options, _jsProvider);
        var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false) };
        await transformer.InitializeAsync(columns);

        // Act & Assert

        // Feed 5 rows
        for (int i = 0; i < 5; i++)
        {
            var res = transformer.TransformMany(new object?[] { i }).ToList();
            res.Should().BeEmpty(); // Should buffer everything
        }

        // Flush
        var resFlush = transformer.Flush().ToList();
        resFlush.Should().HaveCount(1);
        resFlush.First()[0].Should().Be(5d);
    }

    [Fact]
    public async Task ProcessBatch_VaryingKeyGroupSizes_ReusesTemplateSafelyAcrossSizes()
    {
        var options = new WindowOptions
        {
            Key = "Category",
            Script = "const sum = rows.reduce((a, b) => a + b.Val, 0); rows.tag = 'mut'; return [{ Category: rows[0].Category, Val: sum }];"
        };
        var columns = new List<PipeColumnInfo>
        {
            new("Category", typeof(string), false),
            new("Val", typeof(int), false)
        };

        var rowTransformer = new WindowDataTransformer(options, _jsProvider);
        await rowTransformer.InitializeAsync(columns);
        var batchTransformer = new WindowDataTransformer(options, _jsProvider);
        await batchTransformer.InitializeAsync(columns);

        // Groups of size 1, 3, 2, 5, 1.
        var cats = new[] { "A", "B", "B", "B", "C", "C", "D", "D", "D", "D", "D", "E" };
        var vals = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

        var rowOut = new List<(string Cat, double Sum)>();
        for (int i = 0; i < cats.Length; i++)
            rowOut.AddRange(rowTransformer.TransformMany(new object?[] { cats[i], vals[i] }).Select(r => ((string)r[0]!, Convert.ToDouble(r[1]))));
        rowOut.AddRange(rowTransformer.Flush().Select(r => ((string)r[0]!, Convert.ToDouble(r[1]))));

        var catArray = new Apache.Arrow.StringArray.Builder().AppendRange(cats).Build();
        var valArray = new Apache.Arrow.Int32Array.Builder().AppendRange(vals).Build();
        var schema = new Apache.Arrow.Schema(
            [new Apache.Arrow.Field("Category", Apache.Arrow.Types.StringType.Default, false),
             new Apache.Arrow.Field("Val", Apache.Arrow.Types.Int32Type.Default, false)],
            null);
        using var batch = new Apache.Arrow.RecordBatch(schema, [catArray, valArray], cats.Length);

        var batchOut = new List<(string Cat, double Sum)>();
        using (var res = await batchTransformer.TransformBatchAsync(batch))
        {
            if (res is not null)
            {
                var c = (Apache.Arrow.StringArray)res.Column(0);
                var v = (Apache.Arrow.Int32Array)res.Column(1);
                for (int i = 0; i < res.Length; i++) batchOut.Add((c.GetString(i), v.GetValue(i)!.Value));
            }
        }
        await foreach (var flushed in batchTransformer.FlushBatchAsync())
        {
            using (flushed)
            {
                var c = (Apache.Arrow.StringArray)flushed.Column(0);
                var v = (Apache.Arrow.Int32Array)flushed.Column(1);
                for (int i = 0; i < flushed.Length; i++) batchOut.Add((c.GetString(i), v.GetValue(i)!.Value));
            }
        }

        var expected = new (string, double)[] { ("A", 1), ("B", 9), ("C", 11), ("D", 45), ("E", 12) };
        rowOut.Should().Equal(expected);
        batchOut.Should().Equal(expected);
    }
}
