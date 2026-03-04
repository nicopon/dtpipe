using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Resilience;
using Microsoft.Extensions.Logging;

namespace DtPipe.Services;

internal sealed class PipelineExecutor
{
    private readonly IEnumerable<IRowToColumnarBridgeFactory> _bridgeFactories;
    private readonly IEnumerable<IColumnarToRowBridgeFactory> _columnarToRowBridgeFactories;
    private readonly ILogger<PipelineExecutor> _logger;

    public PipelineExecutor(
        IEnumerable<IRowToColumnarBridgeFactory> bridgeFactories,
        IEnumerable<IColumnarToRowBridgeFactory> columnarToRowBridgeFactories,
        ILogger<PipelineExecutor> logger)
    {
        _bridgeFactories = bridgeFactories ?? throw new ArgumentNullException(nameof(bridgeFactories));
        _columnarToRowBridgeFactories = columnarToRowBridgeFactories ?? throw new ArgumentNullException(nameof(columnarToRowBridgeFactories));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    internal static async Task DirectColumnarTransferAsync(
        IAsyncEnumerable<RecordBatch> source,
        IColumnarDataWriter writer,
        int limit,
        IExportProgress progress,
        CancellationToken ct)
    {
        long rowCount = 0;
        await foreach (var batch in source.WithCancellation(ct))
        {
            if (writer.PrefersOwnershipTransfer)
            {
                var batchToWriter = batch;
                progress.ReportRead(batchToWriter.Length);
                await writer.WriteRecordBatchAsync(batchToWriter, ct);
                progress.ReportWrite(batchToWriter.Length);
                rowCount += batchToWriter.Length;
            }
            else
            {
                using (batch) // Ensure Arrow C release callback is called promptly
                {
                    var batchToWriter = batch;
                    if (limit > 0 && rowCount + batch.Length > limit)
                    {
                        // Note: Slicing would be better here if supported.
                    }

                    progress.ReportRead(batchToWriter.Length);
                    await writer.WriteRecordBatchAsync(batchToWriter, ct);
                    progress.ReportWrite(batchToWriter.Length);
                    rowCount += batchToWriter.Length;

                    if (limit > 0 && rowCount >= limit) break;
                }
            }
        }
    }

    internal async Task ExecuteSegmentedPipelineAsync(
        IStreamReader reader,
        IDataWriter writer,
        List<PipelineSegment> segments,
        IReadOnlyList<PipeColumnInfo> columns,
        PipelineOptions options,
        IExportProgress progress,
        RetryPolicy retryPolicy,
        CancellationTokenSource linkedCts,
        CancellationToken ct)
    {
        if (segments.Count == 0)
        {
            if (reader is IColumnarStreamReader cr && writer is IColumnarDataWriter cw)
            {
                var source = cr.ReadRecordBatchesAsync(ct);
                if (options.SamplingRate > 0 && options.SamplingRate < 1.0)
                {
                    var sampler = options.SamplingSeed.HasValue ? new Random(options.SamplingSeed.Value) : Random.Shared;
                    source = ApplySamplingAsync(source, options.SamplingRate, sampler, ct);
                }
                await DirectColumnarTransferAsync(source, cw, options.Limit, progress, ct);
            }
            else if (reader is not IColumnarStreamReader && writer is not IColumnarDataWriter)
            {
                await DirectRowTransferAsync(reader, writer, options.BatchSize, options.Limit, options.SamplingRate, options.SamplingSeed, progress, retryPolicy, ct);
            }
            else
            {
                // Mismatch between Reader and Writer with no transformers
                // Create a dummy segment to force the bridging logic below
                segments.Add(new PipelineSegment(writer is IColumnarDataWriter, new List<IDataTransformer>())
                {
                    InputSchema = columns,
                    OutputSchema = columns
                });
            }
        }

        if (segments.Count > 0)
        {
            IAsyncEnumerable<RecordBatch> currentColumnarSource = null!;
            IAsyncEnumerable<object?[]> currentRowSource = null!;
            bool isCurrentColumnar = false;

            if (reader is IColumnarStreamReader columnarReader)
            {
                currentColumnarSource = columnarReader.ReadRecordBatchesAsync(ct);
                if (options.SamplingRate > 0 && options.SamplingRate < 1.0)
                {
                    var sampler = options.SamplingSeed.HasValue ? new Random(options.SamplingSeed.Value) : Random.Shared;
                    currentColumnarSource = ApplySamplingAsync(currentColumnarSource, options.SamplingRate, sampler, ct);
                }

                // Apply reporting wrapper for the source
                currentColumnarSource = ReportColumnarReadAsync(currentColumnarSource, progress, ct);

                isCurrentColumnar = true;
            }
            else
            {
                currentRowSource = ProduceRowStreamAsync(reader, options.BatchSize, options.Limit, options.SamplingRate, options.SamplingSeed, progress, ct);
                isCurrentColumnar = false;
            }

            foreach (var segment in segments)
            {
                if (segment.IsColumnar)
                {
                    if (!isCurrentColumnar)
                    {
                        var bridgeFac = _bridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No RowToColumnarBridgeFactory");
                        currentColumnarSource = BridgeRowsToColumnarAsync(currentRowSource, bridgeFac, segment.InputSchema, options.BatchSize, ct);
                        isCurrentColumnar = true;
                    }
                    currentColumnarSource = ApplyColumnarSegmentAsync(currentColumnarSource, segment.Transformers, progress, ct);
                }
                else
                {
                    if (isCurrentColumnar)
                    {
                        var bridgeFac = _columnarToRowBridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No ColumnarToRowBridgeFactory");
                        currentRowSource = BridgeColumnarToRowsAsync(currentColumnarSource, bridgeFac, ct);
                        isCurrentColumnar = false;
                    }
                    currentRowSource = ApplyRowSegmentAsync(currentRowSource, segment.Transformers, progress, ct);
                }
            }

            if (writer is IColumnarDataWriter columnarWriter)
            {
                if (!isCurrentColumnar)
                {
                    var bridgeFac = _bridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No RowToColumnarBridgeFactory");
                    currentColumnarSource = BridgeRowsToColumnarAsync(currentRowSource, bridgeFac, columns, options.BatchSize, ct);
                }
                await ConsumeColumnarStreamAsync(currentColumnarSource, columnarWriter, progress, retryPolicy, ct);
            }
            else
            {
                if (isCurrentColumnar)
                {
                    var bridgeFac = _columnarToRowBridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No ColumnarToRowBridgeFactory");
                    currentRowSource = BridgeColumnarToRowsAsync(currentColumnarSource, bridgeFac, ct);
                }
                await ConsumeRowStreamAsync(currentRowSource, writer, options.BatchSize, progress, retryPolicy, ct);
            }
        }
    }

    private async IAsyncEnumerable<object?[]> ProduceRowStreamAsync(
        IStreamReader reader,
        int batchSize,
        int limit,
        double samplingRate,
        int? samplingSeed,
        IExportProgress progress,
        [EnumeratorCancellation] CancellationToken ct)
    {
        Random? sampler = samplingRate > 0 && samplingRate < 1.0 ? (samplingSeed.HasValue ? new Random(samplingSeed.Value) : Random.Shared) : null;
        long rowCount = 0;
        await foreach (var batch in reader.ReadBatchesAsync(batchSize, ct))
        {
            for (int i = 0; i < batch.Length; i++)
            {
                if (sampler != null && sampler.NextDouble() > samplingRate) continue;
                yield return batch.Span[i];
                progress.ReportRead(1);
                if (limit > 0 && ++rowCount >= limit) yield break;
            }
        }
    }

    private async IAsyncEnumerable<RecordBatch> BridgeRowsToColumnarAsync(
        IAsyncEnumerable<object?[]> rows,
        IRowToColumnarBridgeFactory factory,
        IReadOnlyList<PipeColumnInfo> columns,
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var bridge = factory.CreateBridge();
        await bridge.InitializeAsync(columns, batchSize, ct);

        // Feed ingestion in background
        var ingestionTask = Task.Run(async () =>
        {
            try
            {
                var buffer = new List<object?[]>(batchSize);
                await foreach (var row in rows.WithCancellation(ct))
                {
                    buffer.Add(row);
                    if (buffer.Count >= batchSize)
                    {
                        await bridge.IngestRowsAsync(buffer.ToArray(), ct);
                        buffer.Clear();
                    }
                }
                if (buffer.Count > 0)
                {
                    await bridge.IngestRowsAsync(buffer.ToArray(), ct);
                }
                await bridge.CompleteAsync(ct);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during row ingestion in bridge");
                bridge.Fault(ex);
            }
        }, ct);

        // Stream batches as they arrive
        await foreach (var batch in bridge.ReadRecordBatchesAsync(ct))
        {
            yield return batch;
        }

        await ingestionTask;
    }

    private async IAsyncEnumerable<object?[]> BridgeColumnarToRowsAsync(
        IAsyncEnumerable<RecordBatch> batches,
        IColumnarToRowBridgeFactory factory,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var bridge = factory.CreateBridge();
        await foreach (var batch in batches.WithCancellation(ct))
        {
            await foreach (var row in bridge.ConvertBatchToRowsAsync(batch, ct))
            {
                yield return row;
            }
        }
    }

    private async IAsyncEnumerable<RecordBatch> ApplyColumnarSegmentAsync(
        IAsyncEnumerable<RecordBatch> source,
        List<IDataTransformer> transformers,
        IExportProgress progress,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var batch in source.WithCancellation(ct))
        {
            var currentBatch = batch;

            foreach (var t in transformers)
            {
                var transCol = (IColumnarTransformer)t;
                var res = await transCol.TransformBatchAsync(currentBatch, ct);
                if (res != null)
                {
                    progress.ReportTransform(t.GetType().Name.Replace("DataTransformer", ""), res.Length);
                    currentBatch = res;
                }
            }
            yield return currentBatch;
        }

        // Process final flush from stateful transformers
        for (int i = 0; i < transformers.Count; i++)
        {
            var t = (IColumnarTransformer)transformers[i];
            await foreach (var flushedBatch in t.FlushBatchAsync(ct))
            {
                if (flushedBatch == null) continue;
                RecordBatch? current = flushedBatch;

                // Pass flushed batch through subsequent transformers
                for (int j = i + 1; j < transformers.Count; j++)
                {
                    if (current == null) break;
                    var nextT = (IColumnarTransformer)transformers[j];
                    current = await nextT.TransformBatchAsync(current, ct);
                    progress.ReportTransform(nextT.GetType().Name, current?.Length ?? 0);
                }

                if (current != null) yield return current;
            }
        }
    }

    private async IAsyncEnumerable<object?[]> ApplyRowSegmentAsync(
        IAsyncEnumerable<object?[]> source,
        List<IDataTransformer> transformers,
        IExportProgress progress,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var row in source.WithCancellation(ct))
        {
            var results = ProcessRowThroughTransformers(row, transformers, progress, ct);
            foreach (var r in results) yield return r;
        }

        // Process final flush from stateful transformers
        for (int i = 0; i < transformers.Count; i++)
        {
            var t = transformers[i];
            var flushedRows = t.Flush().ToList();
            if (flushedRows.Count > 0)
            {
                var remainingTransformers = transformers.Skip(i + 1).ToList();
                foreach (var fr in flushedRows)
                {
                    var results = ProcessRowThroughTransformers(fr, remainingTransformers, progress, ct);
                    foreach (var r in results) yield return r;
                }
            }
        }
    }

    private List<object?[]> ProcessRowThroughTransformers(
        object?[] row,
        List<IDataTransformer> p,
        IExportProgress progress,
        CancellationToken ct)
    {
        var currentRows = new List<object?[]> { row };
        foreach (var transformer in p)
        {
            var nextRows = new List<object?[]>();
            foreach (var r in currentRows)
            {
                if (transformer is IMultiRowTransformer multi)
                {
                    foreach (var res in multi.TransformMany(r))
                    {
                        if (res != null) { nextRows.Add(res); progress.ReportTransform(transformer.GetType().Name, 1); }
                    }
                }
                else
                {
                    var res = transformer.Transform(r);
                    if (res != null) { nextRows.Add(res); progress.ReportTransform(transformer.GetType().Name, 1); }
                }
            }
            currentRows = nextRows;
            if (currentRows.Count == 0) break;
        }
        return currentRows;
    }

    private async Task ConsumeColumnarStreamAsync(
        IAsyncEnumerable<RecordBatch> source,
        IColumnarDataWriter writer,
        IExportProgress progress,
        RetryPolicy retry,
        CancellationToken ct)
    {
        await foreach (var batch in source.WithCancellation(ct))
        {
            await retry.ExecuteValueAsync(() => writer.WriteRecordBatchAsync(batch, ct), ct);
            progress.ReportWrite(batch.Length);
        }
    }

    private async Task ConsumeRowStreamAsync(
        IAsyncEnumerable<object?[]> source,
        IDataWriter writer,
        int batchSize,
        IExportProgress progress,
        RetryPolicy retry,
        CancellationToken ct)
    {
        var buffer = new List<object?[]>(batchSize);
        await foreach (var row in source.WithCancellation(ct))
        {
            buffer.Add(row);
            if (buffer.Count >= batchSize)
            {
                var batch = buffer.ToArray();
                await retry.ExecuteValueAsync(() => writer.WriteBatchAsync(batch, ct), ct);
                progress.ReportWrite(batch.Length);
                buffer.Clear();
            }
        }
        if (buffer.Count > 0)
        {
            var batch = buffer.ToArray();
            await retry.ExecuteValueAsync(() => writer.WriteBatchAsync(batch, ct), ct);
            progress.ReportWrite(batch.Length);
        }
    }

    private async Task DirectRowTransferAsync(
        IStreamReader reader,
        IDataWriter writer,
        int batchSize,
        int limit,
        double samplingRate,
        int? samplingSeed,
        IExportProgress progress,
        RetryPolicy retry,
        CancellationToken ct)
    {
        long rowCount = 0;
        Random? sampler = samplingRate > 0 && samplingRate < 1.0 ? (samplingSeed.HasValue ? new Random(samplingSeed.Value) : Random.Shared) : null;

        await foreach (var batch in reader.ReadBatchesAsync(batchSize, ct))
        {
            var rowsToWrite = new List<object?[]>();
            for (int i = 0; i < batch.Length; i++)
            {
                if (sampler != null && sampler.NextDouble() > samplingRate) continue;
                rowsToWrite.Add(batch.Span[i]);
                if (limit > 0 && ++rowCount >= limit) break;
            }

            if (rowsToWrite.Count > 0)
            {
                await retry.ExecuteValueAsync(() => writer.WriteBatchAsync(rowsToWrite.ToArray(), ct), ct);
                progress.ReportRead(rowsToWrite.Count);
                progress.ReportWrite(rowsToWrite.Count);
            }
            if (limit > 0 && rowCount >= limit) break;
        }
    }

    private async IAsyncEnumerable<RecordBatch> ApplySamplingAsync(
        IAsyncEnumerable<RecordBatch> source,
        double rate,
        Random sampler,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var batch in source.WithCancellation(ct))
        {
            var sampled = SampleBatch(batch, rate, sampler);
            if (sampled.Length > 0)
            {
                yield return sampled;
            }
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReportColumnarReadAsync(
        IAsyncEnumerable<RecordBatch> source,
        IExportProgress progress,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var batch in source.WithCancellation(ct))
        {
            progress.ReportRead(batch.Length);
            yield return batch;
        }
    }

    private RecordBatch SampleBatch(RecordBatch batch, double rate, Random sampler)
    {
        var selectionVector = new bool[batch.Length];
        int sampledCount = 0;
        for (int i = 0; i < batch.Length; i++)
        {
            if (sampler.NextDouble() <= rate)
            {
                selectionVector[i] = true;
                sampledCount++;
            }
        }

        if (sampledCount == 0)
            return new RecordBatch(batch.Schema, System.Array.Empty<IArrowArray>(), 0);

        if (sampledCount == batch.Length)
            return batch;

        var arrays = new IArrowArray[batch.Schema.FieldsList.Count];
        for (int colIdx = 0; colIdx < batch.Schema.FieldsList.Count; colIdx++)
        {
            var originalArray = batch.Column(colIdx);
            var builder = ArrowTypeMapper.CreateBuilder(originalArray.Data.DataType);

            for (int i = 0; i < originalArray.Length; i++)
            {
                if (selectionVector[i])
                {
                    ArrowTypeMapper.AppendArrayValue(builder, originalArray, i);
                }
            }
            arrays[colIdx] = ArrowTypeMapper.BuildArray(builder);
        }

        return new RecordBatch(batch.Schema, arrays, sampledCount);
    }
}
