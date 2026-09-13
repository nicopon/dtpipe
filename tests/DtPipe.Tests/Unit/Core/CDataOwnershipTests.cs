using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Services;
using DtPipe.Tests.Helpers;
using DtPipe.Transformers.Arrow.Project;
using AwesomeAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// The ownership contract over batches that arrive through the Arrow C Data interface — every
/// <c>duck:</c> read and every <c>--sql</c> result. Their memory is freed by the C release
/// callback, so <see cref="TrackingMemoryPool"/> cannot see it: these assertions count the
/// callback itself, through <see cref="CDataReleaseProbe"/>.
/// </summary>
public class CDataOwnershipTests
{
    private static RecordBatch MakeBatch(int rowCount, int start = 0)
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("val").DataType(DoubleType.Default))
            .Build();

        var idBuilder = new Int32Array.Builder();
        var valBuilder = new DoubleArray.Builder();
        for (int i = 0; i < rowCount; i++)
        {
            idBuilder.Append(start + i);
            valBuilder.Append((start + i) * 1.5);
        }

        return new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), valBuilder.Build() }, rowCount);
    }

    private static List<PipeColumnInfo> Columns() => new()
    {
        new("id", typeof(int), true),
        new("val", typeof(double), true),
    };

    /// <summary>
    /// The instrument itself: a batch nobody disposes is an unreleased batch, and the probe says
    /// so. Without this case the other assertions would pass over a counter that never moves.
    /// </summary>
    [Fact]
    public void AnImportedBatch_IsReleasedByItsConsumer_AndNotBefore()
    {
        using var probe = new CDataReleaseProbe();

        RecordBatch imported;
        using (var source = MakeBatch(8)) imported = probe.Import(source);

        probe.Released.Should().Be(0, "the consumer has not disposed yet");
        ((Int32Array)imported.Column(0)).GetValue(3).Should().Be(3, "the data outlives the managed original");

        imported.Dispose();
        probe.Released.Should().Be(1);
        probe.Outstanding.Should().Be(0);
    }

    /// <summary>
    /// The safety net under a missed dispose, and the reason the process's memory cannot serve as
    /// the instrument here: a dropped imported batch is released by its finalizer, so a consumer
    /// that never disposes costs latency and a finalizer backlog rather than unbounded memory.
    /// A maintainer who reaches for peak RSS instead of this counter measures nothing.
    /// </summary>
    [Fact]
    public void ADroppedImportedBatch_IsReleasedLateByItsFinalizer_NotNever()
    {
        using var probe = new CDataReleaseProbe();

        DropWithoutDisposing(probe, 8);

        probe.Released.Should().Be(0, "nothing has collected yet");

        // Two passes: the first queues the finalizers, the second collects what they freed. More
        // than two only guards against a collection the runtime deferred under load.
        for (int attempt = 0; attempt < 5 && probe.Released < 8; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        probe.Released.Should().Be(8, "the finalizer releases what the consumer forgot");
    }

    // Separate method so no local keeps a batch rooted when the collection runs.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void DropWithoutDisposing(CDataReleaseProbe probe, int count)
    {
        for (int i = 0; i < count; i++)
        {
            using var source = MakeBatch(1024);
            var imported = probe.Import(source);
            imported.Column(0).Length.Should().Be(1024);
        }
    }

    /// <summary>
    /// Fan-out, the shape <c>DagOrchestrator</c> broadcasts with: N retained views, then the
    /// broadcaster drops its own reference. The release must wait for the last consumer —
    /// reference counting has to reach imported buffers, not just allocator-backed ones.
    /// </summary>
    [Fact]
    public void RetainedViews_HoldTheReleaseUntilTheLastConsumerDisposes()
    {
        using var probe = new CDataReleaseProbe();

        RecordBatch imported;
        using (var source = MakeBatch(16)) imported = probe.Import(source);

        var toColumnar = ArrowOwnership.RetainAll(imported);
        var toRows = ArrowOwnership.RetainAll(imported);
        imported.Dispose();

        probe.Released.Should().Be(0, "two consumers still hold retained views");

        ((Int32Array)toColumnar.Column(0)).GetValue(3).Should().Be(3);
        toColumnar.Dispose();
        probe.Released.Should().Be(0, "one consumer is left");

        var rows = ArrowRowConverter.FlattenBatch(toRows, 1024).SelectMany(m => m.ToArray()).ToList();
        rows.Should().HaveCount(16);
        ((int)rows[3]![0]!).Should().Be(3);
        toRows.Dispose();

        probe.Released.Should().Be(1, "the last holder gone releases the C Data memory");
    }

    /// <summary>
    /// The segment runner disposes every input it pulls; with an aliasing transformer the output
    /// keeps the same buffers alive, so the callback fires on the consumer's dispose, once.
    /// </summary>
    [Fact]
    public async Task ApplyColumnarSegmentAsync_ReleasesEveryImportedBatch_ThroughAnAliasingTransformer()
    {
        using var probe = new CDataReleaseProbe();
        var executor = new PipelineExecutor(
            Enumerable.Empty<IRowToColumnarBridgeFactory>(),
            Enumerable.Empty<IColumnarToRowBridgeFactory>(),
            NullLogger<PipelineExecutor>.Instance);

        var project = new ProjectDataTransformer(new ProjectOptions { Rename = new[] { "id:ident" } });
        await project.InitializeAsync(Columns());

        var sources = Enumerable.Range(0, 4).Select(b => MakeBatch(32, b * 32));

        long lastId = -1;
        await foreach (var outBatch in executor.ApplyColumnarSegmentAsync(
            probe.ImportAllAsync(sources, TestContext.Current.CancellationToken),
            new List<IDataTransformer> { project },
            Mock.Of<IExportProgress>(),
            TestContext.Current.CancellationToken))
        {
            outBatch.Schema.FieldsList[0].Name.Should().Be("ident");
            var ids = (Int32Array)outBatch.Column(0);
            lastId = ids.GetValue(ids.Length - 1)!.Value;
            outBatch.Dispose(); // stand in for the writer, the downstream owner
        }

        lastId.Should().Be(127);
        probe.Imported.Should().Be(4);
        probe.Released.Should().Be(4, "the segment runner disposes each input and the consumer each output");
    }

    /// <summary>
    /// The writer boundary, including the <c>--limit</c> slice: <c>SliceShared</c> must
    /// reference-count imported buffers, or the writer's dispose and the runner's would race to
    /// the same release.
    /// </summary>
    [Fact]
    public async Task DrainColumnarSourceAsync_ReleasesEveryImportedBatch_IncludingTheSlicedOne()
    {
        using var probe = new CDataReleaseProbe();
        var executor = new PipelineExecutor(
            Enumerable.Empty<IRowToColumnarBridgeFactory>(),
            Enumerable.Empty<IColumnarToRowBridgeFactory>(),
            NullLogger<PipelineExecutor>.Instance);

        var writer = new RecordingColumnarWriter();
        var sources = Enumerable.Range(0, 3).Select(b => MakeBatch(10, b * 10));

        await executor.DrainColumnarSourceAsync(
            probe.ImportAllAsync(sources, TestContext.Current.CancellationToken),
            writer,
            limit: 15,
            Mock.Of<IExportProgress>(),
            reportReads: true,
            TestContext.Current.CancellationToken);

        writer.Ids.Should().HaveCount(15).And.EndWith(new[] { 14 });
        probe.Imported.Should().Be(2, "the limit stops the source after the second batch");
        probe.Released.Should().Be(2, "the whole batch and the sliced one both reach a single release");
    }

    /// <summary>The terminal row consumer: values readable, then released on the way out.</summary>
    [Fact]
    public async Task ColumnarToRowBridge_ReleasesTheImportedBatch_AfterTheRowsAreMaterialised()
    {
        using var probe = new CDataReleaseProbe();
        var bridge = new ArrowColumnarToRowBridge();

        RecordBatch imported;
        using (var source = MakeBatch(12)) imported = probe.Import(source);

        var rows = new List<IReadOnlyList<object?>>();
        using (imported)
        {
            await foreach (var row in bridge.ConvertBatchToRowsAsync(imported, TestContext.Current.CancellationToken))
                rows.Add(row.ToArray());
        }

        rows.Should().HaveCount(12);
        rows[5][0].Should().Be(5);
        probe.Released.Should().Be(1);
    }

    private sealed class RecordingColumnarWriter : IColumnarDataWriter
    {
        public List<int> Ids { get; } = new();

        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
        {
            // Reads before disposing: a released buffer read here would show as garbage or a crash.
            var ids = (Int32Array)batch.Column(0);
            for (int i = 0; i < ids.Length; i++) Ids.Add(ids.GetValue(i)!.Value);
            batch.Dispose();
            return ValueTask.CompletedTask;
        }

        public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
            => throw new NotSupportedException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
