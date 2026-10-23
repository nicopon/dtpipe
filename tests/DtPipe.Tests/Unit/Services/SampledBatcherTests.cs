using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Services;
using Xunit;

namespace DtPipe.Tests.Unit.Services;

public class SampledBatcherTests
{
    private class FakeRowDataWriter : IRowDataWriter
    {
        public List<object?[]> BatchesWritten { get; } = new();

        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
        {
            var copy = new object?[rows.Count][];
            for (int i = 0; i < rows.Count; i++)
            {
                copy[i] = rows[i];
            }
            BatchesWritten.Add(copy);
            return ValueTask.CompletedTask;
        }

        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask CompleteAsync(CancellationToken ct = default)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private class FakeExportProgress : IExportProgress
    {
        public int ReadsReported { get; private set; }
        public int WritesReported { get; private set; }

        public void ReportRead(int count) => ReadsReported += count;
        public void ReportWrite(int count) => WritesReported += count;
        public void ReportTransform(string name, int count) { }
        public void Complete() { }
        public DtPipe.Core.Models.ExportMetrics GetMetrics() =>
            new(DateTime.UtcNow, DateTime.UtcNow, ReadsReported, WritesReported, 0, 0, new Dictionary<string, long>());
        public void Dispose() { }
    }

    [Fact]
    public async Task ProcessRowAsync_BatchesCorrectly()
    {
        var writer = new FakeRowDataWriter();
        var progress = new FakeExportProgress();
        var batcher = new SampledBatcher(writer, batchSize: 3, limit: 0, samplingRate: 1.0, samplingSeed: null, progress: progress);

        await batcher.ProcessRowAsync(new object?[] { 1, "Alice" }, CancellationToken.None);
        await batcher.ProcessRowAsync(new object?[] { 2, "Bob" }, CancellationToken.None);
        Assert.Empty(writer.BatchesWritten); // Not yet flushed

        await batcher.ProcessRowAsync(new object?[] { 3, "Charlie" }, CancellationToken.None);
        Assert.Single(writer.BatchesWritten); // Flushed because batchSize is 3
        Assert.Equal(3, writer.BatchesWritten[0].Length);

        await batcher.ProcessRowAsync(new object?[] { 4, "David" }, CancellationToken.None);
        await batcher.FlushAsync(CancellationToken.None);

        Assert.Equal(2, writer.BatchesWritten.Count);
        Assert.Single(writer.BatchesWritten[1]);
        Assert.Equal(4, progress.WritesReported);
        Assert.Equal(0, progress.ReadsReported); // reportReads was false
    }

    [Fact]
    public async Task ProcessRowAsync_EnforcesLimitAndThrows()
    {
        var writer = new FakeRowDataWriter();
        var progress = new FakeExportProgress();
        var batcher = new SampledBatcher(writer, batchSize: 10, limit: 2, samplingRate: 1.0, samplingSeed: null, progress: progress);

        await batcher.ProcessRowAsync(new object?[] { 1 }, CancellationToken.None);

        await Assert.ThrowsAsync<LimitReachedException>(() =>
            batcher.ProcessRowAsync(new object?[] { 2 }, CancellationToken.None));

        // It should have flushed because the limit was reached
        Assert.Single(writer.BatchesWritten);
        Assert.Equal(2, writer.BatchesWritten[0].Length);
    }

    [Fact]
    public async Task ProcessRowAsync_ReportsReadsWhenEnabled()
    {
        var writer = new FakeRowDataWriter();
        var progress = new FakeExportProgress();
        var batcher = new SampledBatcher(writer, batchSize: 5, limit: 0, samplingRate: 1.0, samplingSeed: null, progress: progress, reportReads: true);

        await batcher.ProcessRowAsync(new object?[] { 1 }, CancellationToken.None);
        await batcher.ProcessRowAsync(new object?[] { 2 }, CancellationToken.None);

        Assert.Equal(2, progress.ReadsReported);
    }

    [Fact]
    public async Task ProcessRowAsync_FiltersBySamplingRate()
    {
        var writer = new FakeRowDataWriter();
        var progress = new FakeExportProgress();
        // Set sampling seed for deterministic behavior
        var batcher = new SampledBatcher(writer, batchSize: 1, limit: 0, samplingRate: 0.5, samplingSeed: 42, progress: progress);

        for (int i = 0; i < 10; i++)
        {
            await batcher.ProcessRowAsync(new object?[] { i }, CancellationToken.None);
        }
        await batcher.FlushAsync(CancellationToken.None);

        // Under seed 42 with 50% rate, some elements will be filtered out.
        // Let's assert that we wrote fewer than 10 rows.
        Assert.True(progress.WritesReported < 10);
        Assert.True(progress.WritesReported > 0);
    }
}
