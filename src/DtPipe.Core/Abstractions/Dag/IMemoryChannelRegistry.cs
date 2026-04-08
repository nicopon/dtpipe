using DtPipe.Core.Models;
using System.Threading.Channels;
using Apache.Arrow;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// A unified registry for sharing in-memory channels between pipeline branches.
/// Standardized on Apache Arrow RecordBatches for the wire protocol.
/// </summary>
public interface IMemoryChannelRegistry
{
    // --- Arrow Methods (Primary) ---
    void RegisterArrowChannel(string branchAlias, Channel<RecordBatch> channel, Schema schema);
    void UpdateArrowChannelSchema(string branchAlias, Schema schema);
    (Channel<RecordBatch> Channel, Schema Schema)? GetArrowChannel(string branchAlias);
    Task<Schema> WaitForArrowChannelSchemaAsync(string branchAlias, CancellationToken ct = default);

    // --- Row-based Schema Façades (delegate to Arrow storage) ---
    void UpdateChannelColumns(string branchAlias, IReadOnlyList<PipeColumnInfo> columns);
    Task<IReadOnlyList<PipeColumnInfo>> WaitForChannelColumnsAsync(string branchAlias, CancellationToken ct = default);

    // --- Common ---
    bool ContainsChannel(string branchAlias);
}
