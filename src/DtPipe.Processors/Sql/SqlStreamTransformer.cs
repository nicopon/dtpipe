using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Processors.Sql;

/// <summary>
/// An <see cref="IStreamTransformer"/> that adapts an <see cref="IColumnarStreamReader"/> to the
/// stream transformer contract. Engine-agnostic: works with any SQL processor (DuckDB, DataFusion…).
///
/// The transformer reads directly from the upstream Arrow memory channels (registered during
/// <see cref="OpenAsync"/>), so the <c>inputStream</c> parameter of <see cref="ReadResultsAsync"/>
/// is intentionally ignored — the underlying processor consumes channels through its own mechanism.
/// </summary>
public sealed class SqlStreamTransformer : IStreamTransformer
{
    private readonly IColumnarStreamReader _inner;

    public SqlStreamTransformer(IColumnarStreamReader inner)
    {
        _inner = inner;
    }

    /// <inheritdoc/>
    public IReadOnlyList<PipeColumnInfo>? Columns => _inner.Columns;
    public Schema? Schema => _inner.Schema;

    /// <inheritdoc/>
    public async Task OpenAsync(CancellationToken ct = default)
        => await _inner.OpenAsync(ct);

    /// <inheritdoc/>
    /// <remarks>
    /// The <paramref name="inputStream"/> parameter is ignored: the SQL processor reads from
    /// the upstream Arrow channels that were registered during <see cref="OpenAsync"/>.
    /// </remarks>
    public IAsyncEnumerable<RecordBatch> ReadResultsAsync(
        IAsyncEnumerable<RecordBatch>? inputStream = null,
        CancellationToken ct = default)
        => _inner.ReadRecordBatchesAsync(ct);

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}
