using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Engine;

/// <summary>Common surface of the two sample-mode sinks, so callers need not know which one they got.</summary>
internal interface ISampleModeSink
{
    /// <summary>Rows the pipeline delivered to the writer boundary.</summary>
    long RowsWritten { get; }

    /// <summary>The real writer, held for inspection only — never written to.</summary>
    IDataWriter Inner { get; }
}

/// <summary>
/// Swallows writes so a sample run reaches the writer boundary without touching the target.
///
/// It mirrors the real writer's CAPABILITY rather than replacing it. ExecuteSegmentedPipelineAsync
/// picks row or columnar mode from <c>writer is IColumnarDataWriter</c>, and BuildExecutionPlan
/// derives the bridge count the same way — so a sink of the wrong kind would change the
/// segmentation and the number of row/columnar bridges, and the sampled run would no longer be
/// the run. Substituting the null writer, which is row-only, is precisely that mistake.
///
/// Hence two classes chosen by the real writer's interface, the same shape
/// CursorTracking{Row,Columnar}Decorator already uses.
///
/// The inner writer is held for <see cref="ISchemaInspector"/> only. InitializeAsync,
/// the write methods and CompleteAsync are NEVER forwarded: the target is inspected, never
/// mutated. Constructing a writer does not touch its target — an invariant DryRunSafeWriterTests
/// holds for the catalogue — which is what makes inspection safe.
/// </summary>
internal static class SampleModeSink
{
    /// <summary>Wraps <paramref name="inner"/> in the sink matching its capability.</summary>
    public static IDataWriter Wrap(IDataWriter inner)
        => inner is IColumnarDataWriter columnar
            ? new SampleModeColumnarSink(columnar)
            : new SampleModeRowSink(inner);
}

internal sealed class SampleModeRowSink : IRowDataWriter, ISampleModeSink
{
    public SampleModeRowSink(IDataWriter inner) => Inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public IDataWriter Inner { get; }
    public long RowsWritten { get; private set; }

    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
        => ValueTask.CompletedTask;

    public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        RowsWritten += rows.Count;
        return ValueTask.CompletedTask;
    }

    public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

    /// <summary>
    /// A no-op rather than a throw: failing here would make a sample run fail where the real
    /// run succeeds, which is the divergence this whole path exists to remove.
    /// </summary>
    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => Inner.DisposeAsync();
}

internal sealed class SampleModeColumnarSink : IColumnarDataWriter, ISampleModeSink
{
    public SampleModeColumnarSink(IColumnarDataWriter inner) => Inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public IDataWriter Inner { get; }
    public long RowsWritten { get; private set; }

    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
        => ValueTask.CompletedTask;

    /// <summary>
    /// Takes ownership like any columnar writer (.claude/rules/engine-invariants.md › "RecordBatch ownership") and
    /// disposes. Arrow buffers are off-heap: a sink that only dropped the reference would leak
    /// native memory the GC never reports.
    /// </summary>
    public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        RowsWritten += batch.Length;
        batch.Dispose();
        return ValueTask.CompletedTask;
    }

    public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => Inner.DisposeAsync();
}
