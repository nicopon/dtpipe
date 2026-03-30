using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Services;

public class PipelineExecutorTests
{
    private readonly PipelineExecutor _executor;

    public PipelineExecutorTests()
    {
        _executor = new PipelineExecutor(
            Enumerable.Empty<IRowToColumnarBridgeFactory>(),
            Enumerable.Empty<IColumnarToRowBridgeFactory>(),
            NullLogger<PipelineExecutor>.Instance);
    }

    [Fact]
    public async Task ProduceRowStreamAsync_RespectsLimit()
    {
        var mockReader = new Mock<IStreamReader>();
        var batches = new List<ReadOnlyMemory<object?[]>>
        {
            new ReadOnlyMemory<object?[]>(new[] { new object?[] { 1 }, new object?[] { 2 }, new object?[] { 3 } })
        };
        mockReader.Setup(r => r.ReadBatchesAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                  .Returns(batches.ToAsyncEnumerable());

        var mockProgress = new Mock<IExportProgress>();

        var stream = _executor.ProduceRowStreamAsync(mockReader.Object, 10, 2, 0, null, mockProgress.Object, default);
        var result = await stream.ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0][0]);
        Assert.Equal(2, result[1][0]);
    }

    [Fact]
    public async Task ProduceRowStreamAsync_SamplingWithSeed()
    {
        var mockReader = new Mock<IStreamReader>();
        var data = Enumerable.Range(0, 10).Select(i => new object?[] { i }).ToArray();
        var batches = new List<ReadOnlyMemory<object?[]>> { new ReadOnlyMemory<object?[]>(data) };
        mockReader.Setup(r => r.ReadBatchesAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
                  .Returns(batches.ToAsyncEnumerable());

        var mockProgress = new Mock<IExportProgress>();

        // Same seed should yield same results
        var stream1 = _executor.ProduceRowStreamAsync(mockReader.Object, 10, 0, 0.5, 42, mockProgress.Object, default);
        var stream2 = _executor.ProduceRowStreamAsync(mockReader.Object, 10, 0, 0.5, 42, mockProgress.Object, default);

        var result1 = await stream1.ToListAsync();
        var result2 = await stream2.ToListAsync();

        Assert.Equal(result1.Count, result2.Count);
        for (int i = 0; i < result1.Count; i++)
        {
            Assert.Equal(result1[i][0], result2[i][0]);
        }
    }

    [Fact]
    public async Task ConsumeRowStreamAsync_BuffersByBatch()
    {
        var source = new[] { new object?[] { 1 }, new object?[] { 2 }, new object?[] { 3 } }.ToAsyncEnumerable();
        var mockWriter = new Mock<IDataWriter>();
        var mockProgress = new Mock<IExportProgress>();

        await _executor.ConsumeRowStreamAsync(source, mockWriter.Object, 2, mockProgress.Object, default);

        mockWriter.Verify(w => w.WriteBatchAsync(It.Is<object?[][]>(b => b.Length == 2), It.IsAny<CancellationToken>()), Times.Once);
        mockWriter.Verify(w => w.WriteBatchAsync(It.Is<object?[][]>(b => b.Length == 1), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public void ProcessRowThroughTransformers_SequentialExecution()
    {
        var t1 = new Mock<IDataTransformer>();
        t1.Setup(t => t.Transform(It.IsAny<object?[]>())).Returns((object?[] r) => new object?[] { (int)r[0]! + 1 });
        var t2 = new Mock<IDataTransformer>();
        t2.Setup(t => t.Transform(It.IsAny<object?[]>())).Returns((object?[] r) => new object?[] { (int)r[0]! * 2 });

        var transformers = new List<IDataTransformer> { t1.Object, t2.Object };
        var mockProgress = new Mock<IExportProgress>();

        var result = _executor.ProcessRowThroughTransformers(new object?[] { 1 }, transformers, mockProgress.Object, default);

        Assert.Single(result);
        Assert.Equal(4, result[0][0]); // (1+1)*2 = 4
    }

    [Fact]
    public void ProcessRowThroughTransformers_FiltersWhenNull()
    {
        var t1 = new Mock<IDataTransformer>();
        t1.Setup(t => t.Transform(It.IsAny<object?[]>())).Returns((object?[] r) => null);

        var transformers = new List<IDataTransformer> { t1.Object };
        var mockProgress = new Mock<IExportProgress>();

        var result = _executor.ProcessRowThroughTransformers(new object?[] { 1 }, transformers, mockProgress.Object, default);

        Assert.Empty(result);
    }

    [Fact]
    public async Task ApplyColumnarSegmentAsync_FlushesStatefulTransformers()
    {
        var mockT1 = new Mock<IColumnarTransformer>();
        var emptyBatch = new RecordBatch(new Schema(new List<Field>(), null), new IArrowArray[0], 0);
        var flushedBatch = new RecordBatch(new Schema(new List<Field>(), null), new IArrowArray[0], 1);

        mockT1.Setup(t => t.TransformBatchAsync(It.IsAny<RecordBatch>(), It.IsAny<CancellationToken>()))
              .ReturnsAsync(emptyBatch);

        mockT1.Setup(t => t.FlushBatchAsync(It.IsAny<CancellationToken>()))
              .Returns(new[] { flushedBatch }.ToAsyncEnumerable());

        var transformers = new List<IDataTransformer> { mockT1.Object };
        var mockProgress = new Mock<IExportProgress>();

        var source = new[] { emptyBatch }.ToAsyncEnumerable();

        var result = await _executor.ApplyColumnarSegmentAsync(source, transformers, mockProgress.Object, default).ToListAsync();

        Assert.Equal(2, result.Count); // 1 transformed, 1 flushed
        Assert.Equal(0, result[0].Length);
        Assert.Equal(1, result[1].Length);
    }
}
