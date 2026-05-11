using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.DryRun;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

public class DryRunColumnarTests
{
    private class ColumnarOnlyTransformer : BaseColumnarTransformer
    {
        public override bool CanProcessColumnar => true;

        protected override ValueTask<RecordBatch?> TransformBatchSafeAsync(RecordBatch batch, CancellationToken ct = default)
        {
            // Simple transformer that adds 1 to the first column
            var firstCol = (Int32Array)batch.Column(0);
            var builder = new Int32Array.Builder();
            for (int i = 0; i < batch.Length; i++)
            {
                builder.Append(firstCol.GetValue(i)!.Value + 1);
            }
            
            var newSchema = new Schema(new[] { new Field("id", Int32Type.Default, false) }, null);
            return new ValueTask<RecordBatch?>(new RecordBatch(newSchema, new[] { builder.Build() }, batch.Length));
        }

        // We explicitly DO NOT override Transform(row) to simulate columnar-only
    }

    [Fact]
    public async Task AnalyzeAsync_WithColumnarOnlyTransformer_ShouldSucceed()
    {
        // Arrange
        var analyzer = new DryRunAnalyzer();
        var readerMock = new Mock<IStreamReader>();
        
        var columns = new List<PipeColumnInfo> { new("id", typeof(int), false) };
        readerMock.Setup(r => r.Columns).Returns(columns);
        
        var rows = new object?[][] { new object?[] { 10 } };
        readerMock.Setup(r => r.ReadBatchesAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                  .Returns(ToAsyncEnumerable(rows));

        var pipeline = new List<IDataTransformer> { new ColumnarOnlyTransformer() };

        // Act
        var result = await analyzer.AnalyzeAsync(readerMock.Object, pipeline, 1);

        // Assert
        Assert.Single(result.Samples);
        var stages = result.Samples[0].Stages;
        Assert.Equal(2, stages.Count); // Input + Transformer
        Assert.Equal(10, stages[0].Values![0]);
        Assert.Equal(11, stages[1].Values![0]);
    }

    private async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ToAsyncEnumerable(object?[][] rows)
    {
        yield return new ReadOnlyMemory<object?[]>(rows);
        await Task.CompletedTask;
    }
}
