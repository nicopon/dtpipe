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
using Microsoft.Extensions.Logging;

namespace DtPipe.Services;

public sealed class PipelineExecutor
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

    /// <summary>Stage 0 for <see cref="ISampleTap"/>: the reader's own output.</summary>
    private const int ReaderStage = 0;

    /// <summary>
    /// Offers each batch to the tap on its way past. Not owned here: the batch is yielded on
    /// to whoever owns it, and the tap is contractually forbidden from disposing or keeping it.
    /// Returns the source unchanged when there is no tap, so an ordinary run pays nothing.
    /// </summary>
    private static IAsyncEnumerable<RecordBatch> TapBatchesAsync(
        IAsyncEnumerable<RecordBatch> source, ISampleTap? tap, int stageIndex, CancellationToken ct)
        => tap is null ? source : TapBatchesCoreAsync(source, tap, stageIndex, ct);

    private static async IAsyncEnumerable<RecordBatch> TapBatchesCoreAsync(
        IAsyncEnumerable<RecordBatch> source, ISampleTap tap, int stageIndex,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var batch in source.WithCancellation(ct))
        {
            if (tap.WantsMore) tap.OnBatch(stageIndex, batch);
            yield return batch;
        }
    }

    private static IAsyncEnumerable<IReadOnlyList<object?>> TapRowsAsync(
        IAsyncEnumerable<IReadOnlyList<object?>> source, ISampleTap? tap, int stageIndex, CancellationToken ct)
        => tap is null ? source : TapRowsCoreAsync(source, tap, stageIndex, ct);

    private static async IAsyncEnumerable<IReadOnlyList<object?>> TapRowsCoreAsync(
        IAsyncEnumerable<IReadOnlyList<object?>> source, ISampleTap tap, int stageIndex,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var row in source.WithCancellation(ct))
        {
            if (tap.WantsMore) tap.OnRow(stageIndex, row);
            yield return row;
        }
    }

    internal async Task DrainColumnarSourceAsync(
        IAsyncEnumerable<RecordBatch> source,
        IColumnarDataWriter writer,
        int limit,
        IExportProgress progress,
        bool reportReads,
        CancellationToken ct)
    {
        long rowCount = 0;
        await foreach (var batch in source.WithCancellation(ct))
        {
            var batchToWriter = batch;
            bool sliced = false;
            if (limit > 0 && rowCount + batch.Length > limit)
            {
                int remaining = (int)(limit - rowCount);
                // SliceShared (not Slice): the slice reference-counts the buffers so it outlives
                // the original batch, which we dispose right after handing the slice to the writer.
                batchToWriter = batch.SliceShared(0, remaining);
                sliced = true;
            }

            if (reportReads) progress.ReportRead(batchToWriter.Length);
            // The writer takes ownership of batchToWriter and disposes it.
            await writer.WriteRecordBatchAsync(batchToWriter, ct);
            if (sliced) batch.Dispose();
            progress.ReportWrite(batchToWriter.Length);
            rowCount += batchToWriter.Length;

            if (limit > 0 && rowCount >= limit) break;
        }
    }

    internal async Task DirectColumnarTransferAsync(
        IAsyncEnumerable<RecordBatch> source,
        IColumnarDataWriter writer,
        int limit,
        IExportProgress progress,
        CancellationToken ct)
    {
        await DrainColumnarSourceAsync(source, writer, limit, progress, reportReads: true, ct);
    }

    internal async Task ExecuteSegmentedPipelineAsync(
        IStreamReader reader,
        IDataWriter writer,
        List<PipelineSegment> segments,
        IReadOnlyList<PipeColumnInfo> columns,
        PipelineOptions options,
        IExportProgress progress,
        CancellationTokenSource linkedCts,
        CancellationToken ct,
        ISampleTap? tap = null,
        Func<IAsyncEnumerable<RecordBatch>, IAsyncEnumerable<RecordBatch>>? materialise = null)
    {
        // Determine whether any segment is columnar. When there are columnar segments, the pipeline
        // must enter Arrow mode at some point regardless of the writer type — columnar transformers
        // only implement TransformBatchAsync and must not be called via Transform(row).
        // ── Materialisation needs a RecordBatch to tee ────────────────────────────────────
        // A pipeline that is row-mode at the writer boundary has none. Rather than refuse the
        // request, append an EMPTY columnar segment: the existing device — three lines below,
        // a row reader feeding a columnar writer already gets one — and it makes the engine
        // bridge to Arrow at the end of the chain, tee there, and bridge back.
        //
        // The Arrow round-trip this introduces is not an artefact of the bridge: a checkpoint
        // IS Arrow, so resuming from one pays the same conversion whatever we do here. Refusing
        // did not avoid the round-trip, it only prevented checkpointing. (That was the reasoning
        // behind the original refusal, and it was wrong.)
        //
        // It costs an ordinary run nothing, structurally: without --checkpoint the segment is
        // never added and not one branch below sees a difference.
        if (materialise is not null && (segments.Count == 0 || !segments[^1].IsColumnar))
        {
            segments.Add(new PipelineSegment(true, new List<IDataTransformer>())
            {
                InputSchema = columns,
                OutputSchema = columns
            });
            _logger.LogDebug("[Materialise] appended an empty columnar segment to reach Arrow at the writer boundary");
        }

        bool hasColumnarSegments = segments.Any(s => s.IsColumnar);

        if (segments.Count == 0)
        {
            if (reader is IColumnarStreamReader cr && writer is IColumnarDataWriter cw)
            {
                // Both columnar: direct zero-copy transfer
                var source = cr.ReadRecordBatchesAsync(ct);
                if (options.SamplingRate > 0 && options.SamplingRate < 1.0)
                {
                    var sampler = options.SamplingSeed.HasValue ? new Random(options.SamplingSeed.Value) : Random.Shared;
                    source = ApplySamplingAsync(source, options.SamplingRate, sampler, ct);
                }
                source = TapBatchesAsync(source, tap, ReaderStage, ct);
                if (materialise is not null) source = materialise(source);
                await DirectColumnarTransferAsync(source, cw, options.Limit, progress, ct);
            }
            else if (reader is IColumnarStreamReader crForRows && writer is IRowDataWriter rw)
            {
                // Columnar reader → row-mode writer: bridge via existing infrastructure.
                // Do NOT call ReadBatchesAsync — route through ReadRecordBatchesAsync + bridge.
                var bridgeFac = _columnarToRowBridgeFactories.FirstOrDefault()
                    ?? throw new InvalidOperationException("No ColumnarToRowBridgeFactory");
                var batchSource = crForRows.ReadRecordBatchesAsync(ct);
                if (materialise is not null) batchSource = materialise(batchSource);
                var rowSource = TapRowsAsync(
                    BridgeColumnarToRowsAsync(batchSource, bridgeFac, ct), tap, ReaderStage, ct);
                await DirectRowTransferFromRowsAsync(rowSource, rw, options.BatchSize, options.Limit, options.SamplingRate, options.SamplingSeed, progress, ct);
            }
            else if (writer is IRowDataWriter rw2)
            {
                // Row-only reader → row-mode writer: the direct path, with no Arrow anywhere.
                // Materialisation never lands here — the segment appended above guarantees a
                // columnar boundary — so this branch stays exactly as fast as it was.
                await DirectRowTransferAsync(reader, rw2, options.BatchSize, options.Limit, options.SamplingRate, options.SamplingSeed, progress, tap, ct);
            }
            else
            {
                // Row reader + columnar writer with no transformers: bridge rows→Arrow via dummy segment
                segments.Add(new PipelineSegment(true, new List<IDataTransformer>())
                {
                    InputSchema = columns,
                    OutputSchema = columns
                });
            }
        }

        if (segments.Count > 0)
        {
            IAsyncEnumerable<RecordBatch> currentColumnarSource = null!;
            IAsyncEnumerable<IReadOnlyList<object?>> currentRowSource = null!;
            bool isCurrentColumnar = false;

            if (reader is IColumnarStreamReader columnarReader && (writer is IColumnarDataWriter || hasColumnarSegments))
            {
                // Start in Arrow mode when the writer is columnar or when columnar segments are present.
                // This avoids materialising object?[] rows for data that will be processed as RecordBatches.
                currentColumnarSource = columnarReader.ReadRecordBatchesAsync(ct);
                if (options.SamplingRate > 0 && options.SamplingRate < 1.0)
                {
                    var sampler = options.SamplingSeed.HasValue ? new Random(options.SamplingSeed.Value) : Random.Shared;
                    currentColumnarSource = ApplySamplingAsync(currentColumnarSource, options.SamplingRate, sampler, ct);
                }
                currentColumnarSource = ReportColumnarReadAsync(currentColumnarSource, progress, ct);
                currentColumnarSource = TapBatchesAsync(currentColumnarSource, tap, ReaderStage, ct);
                isCurrentColumnar = true;
            }
            else
            {
                // Row-mode sink (or row-only reader): start in row mode — zero bridges needed
                currentRowSource = TapRowsAsync(
                    ProduceRowStreamAsync(reader, options.BatchSize, options.Limit, options.SamplingRate, options.SamplingSeed, progress, ct),
                    tap, ReaderStage, ct);
                isCurrentColumnar = false;
            }

            // Stage numbering for the tap: 0 is the reader, then the transformers in pipeline
            // order. Each segment consumes as many stage numbers as it holds transformers.
            int stageBase = ReaderStage;
            foreach (var segment in segments)
            {
                if (segment.IsColumnar)
                {
                    if (!isCurrentColumnar)
                    {
                        var bridgeFac = _bridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No RowToColumnarBridgeFactory");
                        currentColumnarSource = BridgeRowsToColumnarAsync(currentRowSource, bridgeFac, segment.InputSchema, options.BatchSize, options.MaxBatchBytes, ct, segment.InputSchemaArrow);
                        isCurrentColumnar = true;
                    }
                    currentColumnarSource = ApplyColumnarSegmentAsync(currentColumnarSource, segment.Transformers, progress, ct, tap, stageBase);
                }
                else
                {
                    if (isCurrentColumnar)
                    {
                        var bridgeFac = _columnarToRowBridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No ColumnarToRowBridgeFactory");
                        currentRowSource = BridgeColumnarToRowsAsync(currentColumnarSource, bridgeFac, ct);
                        isCurrentColumnar = false;
                    }
                    currentRowSource = ApplyRowSegmentAsync(currentRowSource, segment.Transformers, progress, ct, tap, stageBase);
                }
                stageBase += segment.Transformers.Count;
            }

            if (writer is IColumnarDataWriter columnarWriter)
            {
                if (!isCurrentColumnar)
                {
                    var bridgeFac = _bridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No RowToColumnarBridgeFactory");
                    // Use the reader's native Arrow schema as an override only when no transformers
                    // changed the schema (i.e. the final column set matches the reader's schema).
                    // If transformers added/removed/renamed columns, the reader schema is stale and
                    // using it as an override would cause an appender count mismatch (IndexOutOfRange).
                    var readerSchema = (reader as IColumnarStreamReader)?.Schema;
                    bool schemaUnchanged = readerSchema != null && readerSchema.FieldsList.Count == columns.Count;
                    var richSchema = schemaUnchanged ? readerSchema : null;
                    currentColumnarSource = BridgeRowsToColumnarAsync(currentRowSource, bridgeFac, columns, options.BatchSize, options.MaxBatchBytes, ct, richSchema);
                }
                // The materialisation point sits at the end of the chain, so a checkpoint holds
                // exactly what the writer would have received — which is what makes resuming
                // from it replay the same rows rather than an earlier approximation of them.
                if (materialise is not null)
                    currentColumnarSource = materialise(currentColumnarSource);
                await ConsumeColumnarStreamAsync(currentColumnarSource, columnarWriter, options.Limit, progress, ct);
            }
            else if (writer is IRowDataWriter rowWriter)
            {
                if (isCurrentColumnar)
                {
                    if (materialise is not null)
                        currentColumnarSource = materialise(currentColumnarSource);
                    var bridgeFac = _columnarToRowBridgeFactories.FirstOrDefault() ?? throw new InvalidOperationException("No ColumnarToRowBridgeFactory");
                    currentRowSource = BridgeColumnarToRowsAsync(currentColumnarSource, bridgeFac, ct);
                }
                await ConsumeRowStreamAsync(currentRowSource, rowWriter, options.BatchSize, progress, ct);
            }
            else
            {
                 throw new InvalidOperationException($"Writer '{writer.GetType().Name}' supports neither IRowDataWriter nor IColumnarDataWriter.");
            }
        }
    }

    internal async IAsyncEnumerable<IReadOnlyList<object?>> ProduceRowStreamAsync(
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

    internal async IAsyncEnumerable<RecordBatch> BridgeRowsToColumnarAsync(
        IAsyncEnumerable<IReadOnlyList<object?>> rows,
        IRowToColumnarBridgeFactory factory,
        IReadOnlyList<PipeColumnInfo> columns,
        int batchSize,
        long maxBatchBytes,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct,
        Schema? richSchema = null)
    {
        var schema = richSchema != null
            ? ArrowSchemaFactory.CreateEnriched(columns, richSchema)
            : ArrowSchemaFactory.Create(columns);

        var buffer = new List<IReadOnlyList<object?>>(batchSize);
        long bufferBytes = 0;
        await foreach (var row in rows.WithCancellation(ct))
        {
            buffer.Add(row);
            if (maxBatchBytes > 0) bufferBytes += EstimateRowBytes(row);
            if (buffer.Count >= batchSize || (maxBatchBytes > 0 && bufferBytes >= maxBatchBytes))
            {
                yield return ArrowRowConverter.ToRecordBatch(schema, buffer, buffer.Count);
                buffer.Clear();
                bufferBytes = 0;
            }
        }
        if (buffer.Count > 0)
        {
            yield return ArrowRowConverter.ToRecordBatch(schema, buffer, buffer.Count);
        }
    }

    /// <summary>
    /// Rough byte estimate for one row buffered before the row→columnar bridge. Variable-width
    /// values count their payload; everything else a flat 8. Only used when a byte cap is set.
    /// </summary>
    private static long EstimateRowBytes(IReadOnlyList<object?> row)
    {
        long total = 0;
        for (int i = 0; i < row.Count; i++)
        {
            total += row[i] switch
            {
                null => 0,
                string s => s.Length,
                byte[] b => b.Length,
                _ => 8,
            };
        }
        return total;
    }

    private async IAsyncEnumerable<IReadOnlyList<object?>> BridgeColumnarToRowsAsync(
        IAsyncEnumerable<RecordBatch> batches,
        IColumnarToRowBridgeFactory factory,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var bridge = factory.CreateBridge();
        await foreach (var batch in batches.WithCancellation(ct))
        {
            using (batch)
            {
                await foreach (var row in bridge.ConvertBatchToRowsAsync(batch, ct))
                {
                    yield return row;
                }
            }
        }
    }

    internal async IAsyncEnumerable<RecordBatch> ApplyColumnarSegmentAsync(
        IAsyncEnumerable<RecordBatch> source,
        List<IDataTransformer> transformers,
        IExportProgress progress,
        [EnumeratorCancellation] CancellationToken ct,
        ISampleTap? tap = null,
        int stageBase = 0)
    {
        // Ownership (see CLAUDE.md › "RecordBatch ownership"): this method owns every batch it
        // pulls from `source`. It disposes each input once the transformer chain has consumed it —
        // a transformer that returns a new batch reusing input columns has retained them, so the
        // dispose is safe. A transformer returning the same reference is pure pass-through; that
        // one object stays live and is disposed downstream, not here.
        await foreach (var batch in source.WithCancellation(ct))
        {
            RecordBatch? currentBatch = batch;

            for (int i = 0; i < transformers.Count; i++)
            {
                var t = transformers[i];
                var transCol = (IColumnarTransformer)t;
                var res = await transCol.TransformBatchAsync(currentBatch!, ct);
                if (!ReferenceEquals(res, currentBatch))
                    currentBatch!.Dispose();
                currentBatch = res;
                if (currentBatch == null) break;
                progress.ReportTransform(t.GetType().Name.Replace("DataTransformer", ""), currentBatch.Length);
                if (tap?.WantsMore == true) tap.OnBatch(stageBase + i + 1, currentBatch);
            }
            if (currentBatch != null)
            {
                yield return currentBatch;
            }
        }

        // Process final flush from stateful transformers. A flushed batch is owned here; run it
        // through the downstream transformers with the same dispose-the-input rule.
        for (int i = 0; i < transformers.Count; i++)
        {
            var t = (IColumnarTransformer)transformers[i];
            await foreach (var flushedBatch in t.FlushBatchAsync(ct))
            {
                if (flushedBatch == null) continue;
                RecordBatch? current = flushedBatch;

                if (tap?.WantsMore == true) tap.OnBatch(stageBase + i + 1, current);

                for (int j = i + 1; j < transformers.Count && current != null; j++)
                {
                    var nextT = (IColumnarTransformer)transformers[j];
                    var res = await nextT.TransformBatchAsync(current, ct);
                    if (!ReferenceEquals(res, current))
                        current.Dispose();
                    current = res;
                    if (current != null)
                    {
                        progress.ReportTransform(nextT.GetType().Name, current.Length);
                        if (tap?.WantsMore == true) tap.OnBatch(stageBase + j + 1, current);
                    }
                }

                if (current != null) yield return current;
            }
        }
    }

    private async IAsyncEnumerable<IReadOnlyList<object?>> ApplyRowSegmentAsync(
        IAsyncEnumerable<IReadOnlyList<object?>> source,
        List<IDataTransformer> transformers,
        IExportProgress progress,
        [EnumeratorCancellation] CancellationToken ct,
        ISampleTap? tap = null,
        int stageBase = 0)
    {
        await foreach (var row in source.WithCancellation(ct))
        {
            var results = ProcessRowThroughTransformers(row, transformers, progress, ct, tap, stageBase);
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
                    if (tap?.WantsMore == true) tap.OnRow(stageBase + i + 1, fr);
                    var results = ProcessRowThroughTransformers(fr, remainingTransformers, progress, ct, tap, stageBase + i + 1);
                    foreach (var r in results) yield return r;
                }
            }
        }
    }

    internal List<IReadOnlyList<object?>> ProcessRowThroughTransformers(
        IReadOnlyList<object?> row,
        List<IDataTransformer> p,
        IExportProgress progress,
        CancellationToken ct,
        ISampleTap? tap = null,
        int stageBase = 0)
    {
        var currentRows = new List<IReadOnlyList<object?>> { row };
        for (int i = 0; i < p.Count; i++)
        {
            var transformer = p[i];
            var nextRows = new List<IReadOnlyList<object?>>();
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
            if (tap?.WantsMore == true)
            {
                var stage = stageBase + i + 1;
                foreach (var r in currentRows) tap.OnRow(stage, r);
            }
            if (currentRows.Count == 0) break;
        }
        return currentRows;
    }

    private async Task ConsumeColumnarStreamAsync(
        IAsyncEnumerable<RecordBatch> source,
        IColumnarDataWriter writer,
        int limit,
        IExportProgress progress,
        CancellationToken ct)
    {
        await DrainColumnarSourceAsync(source, writer, limit, progress, reportReads: false, ct);
    }

    internal async Task ConsumeRowStreamAsync(
        IAsyncEnumerable<IReadOnlyList<object?>> source,
        IRowDataWriter writer,
        int batchSize,
        IExportProgress progress,
        CancellationToken ct)
    {
        var batcher = new SampledBatcher(writer, batchSize, 0, 1.0, null, progress, reportReads: false);
        try
        {
            await foreach (var row in source.WithCancellation(ct))
            {
                await batcher.ProcessRowAsync(row, ct);
            }
        }
        catch (LimitReachedException) { }
        finally
        {
            await batcher.FlushAsync(ct);
        }
    }

    internal async Task DrainRowSourceAsync(
        IAsyncEnumerable<object?[]> rows,
        IRowDataWriter writer,
        int batchSize,
        int limit,
        double samplingRate,
        int? samplingSeed,
        IExportProgress progress,
        CancellationToken ct)
    {
        var batcher = new SampledBatcher(writer, batchSize, limit, samplingRate, samplingSeed, progress, reportReads: true);
        try
        {
            await foreach (var row in rows.WithCancellation(ct))
            {
                await batcher.ProcessRowAsync(row, ct);
            }
        }
        catch (LimitReachedException) { }
        finally
        {
            await batcher.FlushAsync(ct);
        }
    }

    private async Task DirectRowTransferAsync(
        IStreamReader reader,
        IRowDataWriter writer,
        int batchSize,
        int limit,
        double samplingRate,
        int? samplingSeed,
        IExportProgress progress,
        ISampleTap? tap,
        CancellationToken ct)
    {
        async IAsyncEnumerable<object?[]> FlattenBatches([EnumeratorCancellation] CancellationToken innerCt = default)
        {
            await foreach (var batch in reader.ReadBatchesAsync(batchSize, innerCt))
            {
                var arr = batch.ToArray();
                for (int i = 0; i < arr.Length; i++)
                {
                    yield return arr[i];
                }
            }
        }

        async IAsyncEnumerable<object?[]> Tapped([EnumeratorCancellation] CancellationToken innerCt = default)
        {
            await foreach (var row in FlattenBatches(innerCt))
            {
                if (tap?.WantsMore == true) tap.OnRow(ReaderStage, row);
                yield return row;
            }
        }

        await DrainRowSourceAsync(tap is null ? FlattenBatches(ct) : Tapped(ct), writer, batchSize, limit, samplingRate, samplingSeed, progress, ct);
    }

    private async Task DirectRowTransferFromRowsAsync(
        IAsyncEnumerable<IReadOnlyList<object?>> rows,
        IRowDataWriter writer,
        int batchSize,
        int limit,
        double samplingRate,
        int? samplingSeed,
        IExportProgress progress,
        CancellationToken ct)
    {
        async IAsyncEnumerable<object?[]> MaterializeRows([EnumeratorCancellation] CancellationToken innerCt = default)
        {
            await foreach (var r in rows.WithCancellation(innerCt))
            {
                // ArrowRowView is a struct, so the array arm never matches what the
                // columnar→row bridge yields. Its own ToArray sizes the array once; calling
                // ToArray through IReadOnlyList binds to Enumerable.ToArray, which cannot
                // pre-size (the view exposes no ICollection<T>) and grows a buffer through
                // the boxed iterator instead — five times the cost of the allocation it wraps.
                yield return r switch
                {
                    object?[] arr => arr,
                    ArrowRowView view => view.ToArray(),
                    _ => r.ToArray(),
                };
            }
        }

        await DrainRowSourceAsync(MaterializeRows(ct), writer, batchSize, limit, samplingRate, samplingSeed, progress, ct);
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
            if (ReferenceEquals(sampled, batch))
            {
                yield return sampled;
                continue;
            }
            // SampleBatch built a new batch (or an empty one): the input is ours to dispose.
            batch.Dispose();
            if (sampled.Length > 0)
                yield return sampled;
            else
                sampled.Dispose();
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
