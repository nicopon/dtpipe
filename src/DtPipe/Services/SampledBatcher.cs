using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Core.Abstractions;

namespace DtPipe.Services;

internal class SampledBatcher
{
    private readonly IRowDataWriter _writer;
    private readonly int _batchSize;
    private readonly int _limit;
    private readonly double _samplingRate;
    private readonly Random? _sampler;
    private readonly IExportProgress _progress;
    private readonly bool _reportReads;
    private readonly List<object?[]> _buffer;
    private long _rowCount;

    public SampledBatcher(
        IRowDataWriter writer,
        int batchSize,
        int limit,
        double samplingRate,
        int? samplingSeed,
        IExportProgress progress,
        bool reportReads = false)
    {
        _writer = writer ?? throw new ArgumentNullException(nameof(writer));
        _batchSize = batchSize;
        _limit = limit;
        _samplingRate = samplingRate;
        _sampler = samplingRate > 0 && samplingRate < 1.0
            ? (samplingSeed.HasValue ? new Random(samplingSeed.Value) : Random.Shared)
            : null;
        _progress = progress ?? throw new ArgumentNullException(nameof(progress));
        _reportReads = reportReads;
        _buffer = new List<object?[]>(batchSize);
    }

    public async Task ProcessRowAsync(IReadOnlyList<object?> row, CancellationToken ct)
    {
        if (_sampler != null && _sampler.NextDouble() > _samplingRate) return;

        var rowArray = row as object?[] ?? row.ToArray();
        _buffer.Add(rowArray);
        if (_reportReads)
        {
            _progress.ReportRead(1);
        }

        bool limitReached = _limit > 0 && ++_rowCount >= _limit;

        if (_buffer.Count >= _batchSize || limitReached)
        {
            await FlushBatchAsync(ct);
        }

        if (limitReached)
        {
            throw new LimitReachedException();
        }
    }

    public async Task FlushAsync(CancellationToken ct)
    {
        if (_buffer.Count > 0)
        {
            await FlushBatchAsync(ct);
        }
    }

    private async Task FlushBatchAsync(CancellationToken ct)
    {
        var batch = _buffer.ToArray();
        await _writer.WriteBatchAsync(batch, ct);
        _progress.ReportWrite(batch.Length);
        _buffer.Clear();
    }
}

internal class LimitReachedException : Exception
{
    public LimitReachedException() : base("Limit reached") { }
}
