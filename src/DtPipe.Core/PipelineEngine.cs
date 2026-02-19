using System.Threading.Channels;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Resilience;
using Microsoft.Extensions.Logging;

namespace DtPipe.Core;

/// <summary>
/// Headless pipeline engine: reads from a source, optionally transforms, writes to a destination.
/// Does not depend on any UI / CLI layer. Designed for direct library consumption.
/// </summary>
public class PipelineEngine
{
    private readonly ILogger<PipelineEngine> _logger;

    public PipelineEngine(ILogger<PipelineEngine> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Runs the full read → [transform] → write pipeline.
    /// </summary>
    /// <param name="reader">Open and initialized stream reader. Caller must call OpenAsync before passing it.</param>
    /// <param name="writer">Initialized data writer. Caller must call InitializeAsync before passing it.</param>
    /// <param name="pipeline">Ordered list of transformers to apply (may be empty).</param>
    /// <param name="batchSize">Number of rows per batch. Default: 50 000.</param>
    /// <param name="limit">Max rows to process. 0 = unlimited.</param>
    /// <param name="samplingRate">Row sampling probability 0.0–1.0. 1.0 = all rows.</param>
    /// <param name="samplingSeed">Optional seed for reproducible sampling.</param>
    /// <param name="maxRetries">Number of retries on transient write errors. Default: 3.</param>
    /// <param name="retryDelayMs">Initial delay between retries in ms (doubles each attempt). Default: 1000.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Total number of rows written.</returns>
    public async Task<long> RunAsync(
        IStreamReader reader,
        IDataWriter writer,
        IReadOnlyList<IDataTransformer>? pipeline = null,
        int batchSize = 50_000,
        int limit = 0,
        double samplingRate = 1.0,
        int? samplingSeed = null,
        int maxRetries = 3,
        int retryDelayMs = 1000,
        CancellationToken ct = default)
    {
        pipeline ??= Array.Empty<IDataTransformer>();
        var retryPolicy = new RetryPolicy(maxRetries, TimeSpan.FromMilliseconds(retryDelayMs), _logger);

        var readerToTransform = Channel.CreateBounded<object?[]>(new BoundedChannelOptions(1000)
        {
            SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait
        });

        var transformToWriter = Channel.CreateBounded<object?[][]>(new BoundedChannelOptions(100)
        {
            SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait
        });

        long totalRows = 0;
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var effectiveCt = linkedCts.Token;

        var producerTask = ProduceRowsAsync(reader, readerToTransform.Writer, batchSize, limit, samplingRate, samplingSeed, linkedCts, effectiveCt, retryPolicy, _logger);
        var transformTask = TransformRowsAsync(readerToTransform.Reader, transformToWriter.Writer, pipeline, batchSize, effectiveCt);
        var consumerTask = ConsumeRowsAsync(transformToWriter.Reader, writer, r => Interlocked.Add(ref totalRows, r), effectiveCt, retryPolicy, _logger);

        var tasks = new List<Task> { producerTask, transformTask, consumerTask };
        while (tasks.Count > 0)
        {
            var finished = await Task.WhenAny(tasks);
            if (finished.IsFaulted)
            {
                await linkedCts.CancelAsync();
                await finished;
            }
            else if (finished.IsCanceled)
            {
                await linkedCts.CancelAsync();
            }
            tasks.Remove(finished);
        }

        await writer.CompleteAsync(ct);
        return totalRows;
    }

    private static async Task ProduceRowsAsync(
        IStreamReader reader,
        ChannelWriter<object?[]> output,
        int batchSize,
        int limit,
        double samplingRate,
        int? samplingSeed,
        CancellationTokenSource linkedCts,
        CancellationToken ct,
        RetryPolicy retryPolicy,
        ILogger logger)
    {
        Random? sampler = null;
        if (samplingRate > 0 && samplingRate < 1.0)
            sampler = samplingSeed.HasValue ? new Random(samplingSeed.Value) : Random.Shared;

        long rowCount = 0;
        try
        {
            await foreach (var batchChunk in reader.ReadBatchesAsync(batchSize, ct))
            {
                for (var i = 0; i < batchChunk.Length; i++)
                {
                    if (sampler != null && sampler.NextDouble() > samplingRate) continue;
                    await output.WriteAsync(batchChunk.Span[i], ct);
                    rowCount++;
                    if (limit > 0 && rowCount >= limit) return;
                }
            }
        }
        catch (OperationCanceledException) when (limit > 0 && rowCount >= limit) { }
        finally { output.Complete(); }
    }

    private static async Task TransformRowsAsync(
        ChannelReader<object?[]> input,
        ChannelWriter<object?[][]> output,
        IReadOnlyList<IDataTransformer> pipeline,
        int batchSize,
        CancellationToken ct)
    {
        try
        {
            await using var batchWriter = new BatchChannelWriter(output, batchSize, ct);
            await foreach (var row in input.ReadAllAsync(ct))
                await ProcessPipelineAsync(row, 0, pipeline, batchWriter, ct);

            for (int i = 0; i < pipeline.Count; i++)
            {
                foreach (var row in pipeline[i].Flush())
                    if (row != null)
                        await ProcessPipelineAsync(row, i + 1, pipeline, batchWriter, ct);
            }
        }
        finally { output.Complete(); }
    }

    private static async ValueTask ProcessPipelineAsync(
        object?[] currentRow,
        int stepIndex,
        IReadOnlyList<IDataTransformer> pipeline,
        BatchChannelWriter finalOutput,
        CancellationToken ct)
    {
        if (stepIndex >= pipeline.Count)
        {
            await finalOutput.WriteAsync(currentRow);
            return;
        }
        var transformer = pipeline[stepIndex];
        if (transformer is IMultiRowTransformer multi)
        {
            foreach (var r in multi.TransformMany(currentRow))
                if (r != null)
                    await ProcessPipelineAsync(r, stepIndex + 1, pipeline, finalOutput, ct);
        }
        else
        {
            var r = transformer.Transform(currentRow);
            if (r != null)
                await ProcessPipelineAsync(r, stepIndex + 1, pipeline, finalOutput, ct);
        }
    }

    private static async Task ConsumeRowsAsync(
        ChannelReader<object?[][]> input,
        IDataWriter writer,
        Action<int> updateRowCount,
        CancellationToken ct,
        RetryPolicy retryPolicy,
        ILogger logger)
    {
        await foreach (var batch in input.ReadAllAsync(ct))
        {
            await retryPolicy.ExecuteValueAsync(() => writer.WriteBatchAsync(batch, ct), ct);
            updateRowCount(batch.Length);
        }
    }

    private sealed class BatchChannelWriter : IAsyncDisposable
    {
        private readonly ChannelWriter<object?[][]> _target;
        private readonly int _batchSize;
        private readonly List<object?[]> _buffer;
        private readonly CancellationToken _ct;

        public BatchChannelWriter(ChannelWriter<object?[][]> target, int batchSize, CancellationToken ct)
        {
            _target = target;
            _batchSize = batchSize;
            _buffer = new List<object?[]>(batchSize);
            _ct = ct;
        }

        public async ValueTask WriteAsync(object?[] row)
        {
            _buffer.Add(row);
            if (_buffer.Count >= _batchSize) await FlushAsync();
        }

        private async ValueTask FlushAsync()
        {
            if (_buffer.Count == 0) return;
            await _target.WriteAsync(_buffer.ToArray(), _ct);
            _buffer.Clear();
        }

        public async ValueTask DisposeAsync() => await FlushAsync();
    }
}
