using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
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
        var mockWriter = new Mock<IRowDataWriter>();
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

    [Fact]
    public async Task BridgeRowsToColumnarAsync_FlushesOnByteCap_BeforeRowCount()
    {
        var columns = new List<PipeColumnInfo> { new("payload", typeof(string), true) };

        // 10 rows, each ~1 KB. Row cap is 100 000; byte cap is 3 KB → expect a flush every 3 rows.
        async IAsyncEnumerable<IReadOnlyList<object?>> Rows()
        {
            for (int i = 0; i < 10; i++)
            {
                yield return new object?[] { new string('x', 1000) };
                await Task.Yield();
            }
        }

        var batches = new List<int>();
        await foreach (var b in _executor.BridgeRowsToColumnarAsync(
            Rows(), factory: null!, columns, batchSize: 100_000, maxBatchBytes: 3_000, default))
        {
            batches.Add(b.Length);
            b.Dispose();
        }

        Assert.Equal(new[] { 3, 3, 3, 1 }, batches);
    }

    [Fact]
    public async Task BridgeRowsToColumnarAsync_NoByteCap_FlushesOnRowCountOnly()
    {
        var columns = new List<PipeColumnInfo> { new("payload", typeof(string), true) };

        async IAsyncEnumerable<IReadOnlyList<object?>> Rows()
        {
            for (int i = 0; i < 10; i++)
            {
                yield return new object?[] { new string('x', 1000) };
                await Task.Yield();
            }
        }

        var batches = new List<int>();
        await foreach (var b in _executor.BridgeRowsToColumnarAsync(
            Rows(), factory: null!, columns, batchSize: 4, maxBatchBytes: 0, default))
        {
            batches.Add(b.Length);
            b.Dispose();
        }

        Assert.Equal(new[] { 4, 4, 2 }, batches);
    }

    [Fact]
    public async Task DirectColumnarTransferAsync_RespectsLimitAndSlices()
    {
        var schema = new Schema.Builder().Field(f => f.Name("Value").DataType(Int32Type.Default)).Build();
        var array = new Int32Array.Builder().AppendRange(Enumerable.Range(1, 10)).Build();
        var batch = new RecordBatch(schema, new[] { array }, 10);
        var batches = new List<RecordBatch> { batch }.ToAsyncEnumerable();

        var mockWriter = new Mock<IColumnarDataWriter>();
        RecordBatch? writtenBatch = null;
        mockWriter.Setup(w => w.WriteRecordBatchAsync(It.IsAny<RecordBatch>(), It.IsAny<CancellationToken>()))
                  .Callback<RecordBatch, CancellationToken>((b, _) => writtenBatch = b)
                  .Returns(new ValueTask());

        var mockProgress = new Mock<IExportProgress>();

        await _executor.DirectColumnarTransferAsync(batches, mockWriter.Object, 3, mockProgress.Object, default);

        Assert.NotNull(writtenBatch);
        Assert.Equal(3, writtenBatch.Length);
        var values = (Int32Array)writtenBatch.Column(0);
        Assert.Equal(1, values.GetValue(0));
        Assert.Equal(2, values.GetValue(1));
        Assert.Equal(3, values.GetValue(2));
    }

    [Fact]
    public async Task ExecuteSegmentedPipelineAsync_ColumnarPath_RespectsLimitAndSlices()
    {
        var schema = new Schema.Builder().Field(f => f.Name("Value").DataType(Int32Type.Default)).Build();
        var array = new Int32Array.Builder().AppendRange(Enumerable.Range(1, 10)).Build();
        var batch = new RecordBatch(schema, new[] { array }, 10);

        var mockReader = new Mock<IColumnarStreamReader>();
        mockReader.As<IStreamReader>();
        mockReader.Setup(r => r.ReadRecordBatchesAsync(It.IsAny<CancellationToken>()))
                  .Returns(new[] { batch }.ToAsyncEnumerable());
        mockReader.SetupGet(r => r.Schema).Returns(schema);

        var mockWriter = new Mock<IColumnarDataWriter>();
        mockWriter.As<IRowDataWriter>();
        mockWriter.As<IDataWriter>();
        RecordBatch? writtenBatch = null;
        mockWriter.Setup(w => w.WriteRecordBatchAsync(It.IsAny<RecordBatch>(), It.IsAny<CancellationToken>()))
                  .Callback<RecordBatch, CancellationToken>((b, _) => writtenBatch = b)
                  .Returns(new ValueTask());

        var segments = new List<PipelineSegment> { new PipelineSegment(true, new List<IDataTransformer>()) };
        var columns = new List<PipeColumnInfo> { new PipeColumnInfo("Value", typeof(int), true, false) };
        var options = new PipelineOptions { Limit = 3 };
        var mockProgress = new Mock<IExportProgress>();
        using var linkedCts = new CancellationTokenSource();

        await _executor.ExecuteSegmentedPipelineAsync(
            mockReader.Object, 
            (IDataWriter)mockWriter.Object, 
            segments, 
            columns, 
            options, 
            mockProgress.Object, 
            linkedCts, 
            default);

        Assert.NotNull(writtenBatch);
        Assert.Equal(3, writtenBatch.Length);
        var values = (Int32Array)writtenBatch.Column(0);
        Assert.Equal(1, values.GetValue(0));
        Assert.Equal(2, values.GetValue(1));
        Assert.Equal(3, values.GetValue(2));
    }

    /// <summary>
    /// The combination the columnar limit test above does not reach: a columnar reader, a columnar
    /// segment, and a ROW writer. The bound used to live only on the two paths that happened to
    /// carry it — <see cref="PipelineExecutor.ProduceRowStreamAsync"/> and the columnar consumer —
    /// and this shape has neither, so --limit 10 wrote a million rows.
    /// </summary>
    [Fact]
    public async Task ExecuteSegmentedPipelineAsync_ColumnarReader_RowWriter_RespectsLimit()
    {
        var schema = new Schema.Builder().Field(f => f.Name("Value").DataType(Int32Type.Default)).Build();
        var array = new Int32Array.Builder().AppendRange(Enumerable.Range(1, 10)).Build();
        var batch = new RecordBatch(schema, new[] { array }, 10);

        var mockReader = new Mock<IColumnarStreamReader>();
        mockReader.As<IStreamReader>();
        mockReader.Setup(r => r.ReadRecordBatchesAsync(It.IsAny<CancellationToken>()))
                  .Returns(new[] { batch }.ToAsyncEnumerable());
        mockReader.SetupGet(r => r.Schema).Returns(schema);

        // Row writer only: no IColumnarDataWriter, so the run ends on ConsumeRowStreamAsync.
        var mockWriter = new Mock<IRowDataWriter>();
        mockWriter.As<IDataWriter>();
        var written = new List<object?[]>();
        mockWriter.Setup(w => w.WriteBatchAsync(It.IsAny<IReadOnlyList<object?[]>>(), It.IsAny<CancellationToken>()))
                  .Callback<IReadOnlyList<object?[]>, CancellationToken>((b, _) => written.AddRange(b))
                  .Returns(new ValueTask());

        var executor = new PipelineExecutor(
            [new DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory(
                NullLogger<DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge>.Instance)],
            [new DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory()],
            NullLogger<PipelineExecutor>.Instance);

        var segments = new List<PipelineSegment> { new PipelineSegment(true, new List<IDataTransformer>()) };
        var columns = new List<PipeColumnInfo> { new PipeColumnInfo("Value", typeof(int), true, false) };
        using var linkedCts = new CancellationTokenSource();

        await executor.ExecuteSegmentedPipelineAsync(
            mockReader.Object, (IDataWriter)mockWriter.Object, segments, columns,
            new PipelineOptions { Limit = 3 }, new Mock<IExportProgress>().Object, linkedCts, default);

        Assert.Equal(3, written.Count);
        Assert.Equal(new object?[] { 1, 2, 3 }, written.Select(r => r[0]));
    }

    /// <summary>
    /// --limit counts rows READ, so a filter downstream of it yields fewer, never a topped-up N.
    /// Applying the bound a second time at the writer would answer the other question, and the two
    /// disagree exactly here.
    /// </summary>
    [Fact]
    public async Task ExecuteSegmentedPipelineAsync_LimitBoundsRowsRead_NotRowsWritten()
    {
        var schema = new Schema.Builder().Field(f => f.Name("Value").DataType(Int32Type.Default)).Build();
        var array = new Int32Array.Builder().AppendRange(Enumerable.Range(1, 100)).Build();
        var batch = new RecordBatch(schema, new[] { array }, 100);

        var mockReader = new Mock<IColumnarStreamReader>();
        mockReader.As<IStreamReader>();
        mockReader.Setup(r => r.ReadRecordBatchesAsync(It.IsAny<CancellationToken>()))
                  .Returns(new[] { batch }.ToAsyncEnumerable());
        mockReader.SetupGet(r => r.Schema).Returns(schema);

        var mockWriter = new Mock<IColumnarDataWriter>();
        mockWriter.As<IDataWriter>();
        long writtenRows = 0;
        mockWriter.Setup(w => w.WriteRecordBatchAsync(It.IsAny<RecordBatch>(), It.IsAny<CancellationToken>()))
                  .Callback<RecordBatch, CancellationToken>((b, _) => writtenRows += b.Length)
                  .Returns(new ValueTask());

        // Keeps only even values: of the first 10 rows read, 5 survive.
        var dropOdds = new Mock<IDataTransformer>();
        dropOdds.Setup(t => t.Transform(It.IsAny<IReadOnlyList<object?>>()))
                .Returns<IReadOnlyList<object?>>(r => Convert.ToInt32(r[0]) % 2 == 0 ? r.ToArray() : null);

        var executor = new PipelineExecutor(
            [new DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory(
                NullLogger<DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge>.Instance)],
            [new DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory()],
            NullLogger<PipelineExecutor>.Instance);

        var segments = new List<PipelineSegment> { new PipelineSegment(false, new List<IDataTransformer> { dropOdds.Object }) };
        var columns = new List<PipeColumnInfo> { new PipeColumnInfo("Value", typeof(int), true, false) };
        using var linkedCts = new CancellationTokenSource();

        await executor.ExecuteSegmentedPipelineAsync(
            mockReader.Object, (IDataWriter)mockWriter.Object, segments, columns,
            new PipelineOptions { Limit = 10 }, new Mock<IExportProgress>().Object, linkedCts, default);

        Assert.Equal(5, writtenRows);
    }
}
