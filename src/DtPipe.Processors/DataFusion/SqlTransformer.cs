using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.DataFusion;

/// <summary>
/// An <see cref="IStreamTransformer"/> that executes a SQL query via the DataFusion engine.
/// Wraps <see cref="DataFusionProcessor"/> and adapts it to the stream transformer contract.
///
/// The transformer reads directly from the upstream Arrow memory channels (registered during
/// <see cref="OpenAsync"/>), so the <c>inputStream</c> parameter of <see cref="ReadResultsAsync"/>
/// is intentionally ignored — DataFusion consumes the channels through the C Arrow Data Interface.
/// </summary>
public sealed class SqlTransformer : IStreamTransformer
{
    private readonly DataFusionProcessor _inner;

    public SqlTransformer(DataFusionProcessor inner)
    {
        _inner = inner;
    }

    /// <inheritdoc/>
    public IReadOnlyList<PipeColumnInfo>? Columns => _inner.Columns;

    /// <inheritdoc/>
    public async Task OpenAsync(CancellationToken ct = default)
        => await _inner.OpenAsync(ct);

    /// <inheritdoc/>
    /// <remarks>
    /// The <paramref name="inputStream"/> parameter is ignored: DataFusion reads from
    /// the upstream Arrow channels that were registered during <see cref="OpenAsync"/>.
    /// </remarks>
    public IAsyncEnumerable<RecordBatch> ReadResultsAsync(
        IAsyncEnumerable<RecordBatch>? inputStream = null,
        CancellationToken ct = default)
        => _inner.ReadRecordBatchesAsync(ct);

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}
