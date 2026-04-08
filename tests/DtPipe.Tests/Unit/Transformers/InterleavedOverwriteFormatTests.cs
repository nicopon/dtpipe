using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Format;
using DtPipe.Transformers.Arrow.Overwrite;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// Regression tests for the interleaved overwrite+format FormatException bug.
///
/// Root cause: ExportService propagated the reader's original Arrow schema (e.g. Int64 for a
/// generated numeric column) as overrideSchema for ALL pipeline segments. When Overwrite mutated
/// column A from Int64 → string, the next segment's ArrowRowToColumnarBridge still received
/// the stale Int64 overrideSchema → created an Int64Array.Builder → FormatException on "Val1".
///
/// Fix: BuildAndEnrichSchema in ArrowRowToColumnarBridge uses PipeColumnInfo as the type
/// authority. overrideSchema fields are only accepted when the base Arrow type matches.
/// </summary>
public class InterleavedOverwriteFormatTests
{
    /// <summary>
    /// Full pipeline scenario: Overwrite A→Val1, Format B←{A}, Overwrite A→Val2, Format C←{A}.
    /// Simulates a stale reader schema (Int64 for all columns) being passed as overrideSchema
    /// to segments 2 and 3 — the exact condition that caused the FormatException.
    /// Expected: A=Val2, B=Val1, C=Val2.
    /// </summary>
    [Fact]
    public async Task Pipeline_ShouldProduceCorrectValues_WhenOverwriteAndFormatAreInterleaved()
    {
        // Initial schema: A, B, C as string (as they are after the first Overwrite runs)
        var columns = new List<PipeColumnInfo>
        {
            new("A", typeof(string), true),
            new("B", typeof(string), true),
            new("C", typeof(string), true),
        };
        var row = new object?[] { "original_A", "original_B", "original_C" };

        // Stage 1: Overwrite A = "Val1"
        var overwrite1 = new OverwriteDataTransformer(new OverwriteOptions { Overwrite = ["A:Val1"] });
        var cols1 = (await overwrite1.InitializeAsync(columns)).ToList();
        var batch1 = TestBatchBuilder.FromRows(columns, row);
        var result1 = await overwrite1.TransformBatchAsync(batch1);
        result1.Should().NotBeNull();
        TestBatchBuilder.GetVal(result1!, 0, 0).Should().Be("Val1", "Overwrite1 sets A=Val1");

        // Extract row from result1 for next stage
        var row1 = new object?[]
        {
            TestBatchBuilder.GetVal(result1!, 0, 0),
            TestBatchBuilder.GetVal(result1!, 1, 0),
            TestBatchBuilder.GetVal(result1!, 2, 0),
        };

        // Stage 2: Format B = "{A}" (cross-column → row mode)
        // This bridges columnar→rows. The next segment then bridges rows→columnar.
        // The stale overrideSchema passed to that bridge has Int64 for column A.
        var format1 = new FormatDataTransformer(new FormatOptions { Format = ["B:{A}"] });
        var cols2 = (await format1.InitializeAsync(cols1)).ToList();
        var transformedRow1 = format1.Transform(row1)!;
        transformedRow1[1].Should().Be("Val1", "Format1 sets B = value of A = Val1");

        // Stage 3: Overwrite A = "Val2" — arriving via row→columnar bridge
        // The stale schema has Int64 for A; PipeColumnInfo says string. Bridge must use PipeColumnInfo.
        var overwrite2 = new OverwriteDataTransformer(new OverwriteOptions { Overwrite = ["A:Val2"] });
        var cols3 = (await overwrite2.InitializeAsync(cols2)).ToList();

        // Simulate the row→columnar bridge with a stale Int64 overrideSchema (the bug trigger)
        var staleSchema = new Schema(new[]
        {
            new Field("A", Int64Type.Default, true),
            new Field("B", Int64Type.Default, true),
            new Field("C", Int64Type.Default, true),
        }, null);

        var bridge = new ArrowRowToColumnarBridge();
        await bridge.InitializeAsync(cols3, batchSize: 10, overrideSchema: staleSchema);

        // This must NOT throw FormatException despite the stale Int64 overrideSchema
        var ingestTask = Task.Run(async () =>
        {
            await bridge.IngestRowsAsync(new ReadOnlyMemory<object?[]>([transformedRow1]));
            await bridge.CompleteAsync();
        });

        var bridgedBatches = await bridge.ReadRecordBatchesAsync().ToListAsync();
        await ingestTask;

        bridgedBatches.Should().HaveCount(1);
        var bridgedBatch = bridgedBatches[0];
        bridgedBatch.Schema.GetFieldByIndex(0).DataType.Should().BeOfType<StringType>(
            "PipeColumnInfo declares string — stale Int64Type must not win");

        var result3 = await overwrite2.TransformBatchAsync(bridgedBatch);
        result3.Should().NotBeNull();
        TestBatchBuilder.GetVal(result3!, 0, 0).Should().Be("Val2", "Overwrite2 sets A=Val2");
        TestBatchBuilder.GetVal(result3!, 1, 0).Should().Be("Val1", "B retains Val1 from Format1");

        // Stage 4: Format C = "{A}" → C = Val2
        var row3 = new object?[]
        {
            TestBatchBuilder.GetVal(result3!, 0, 0),
            TestBatchBuilder.GetVal(result3!, 1, 0),
            TestBatchBuilder.GetVal(result3!, 2, 0),
        };
        var format2 = new FormatDataTransformer(new FormatOptions { Format = ["C:{A}"] });
        await format2.InitializeAsync(cols3);
        var transformedRow3 = format2.Transform(row3)!;

        transformedRow3[0].Should().Be("Val2", "A = Val2");
        transformedRow3[1].Should().Be("Val1", "B = Val1");
        transformedRow3[2].Should().Be("Val2", "C = value of A = Val2");
    }
}
