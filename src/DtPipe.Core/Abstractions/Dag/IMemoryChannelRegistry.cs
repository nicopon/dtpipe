using DtPipe.Core.Models;
using System.Threading.Channels;
using Apache.Arrow;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// A unified registry for sharing in-memory channels between pipeline branches.
/// Standardized on Apache Arrow RecordBatches for the wire protocol.
/// Row-based methods are provided as bridges for backward compatibility.
/// </summary>
public interface IMemoryChannelRegistry
{
    // --- Arrow Methods (Primary) ---
    void RegisterArrowChannel(string branchAlias, Channel<RecordBatch> channel, Schema schema);
    void UpdateArrowChannelSchema(string branchAlias, Schema schema);
    (Channel<RecordBatch> Channel, Schema Schema)? GetArrowChannel(string branchAlias);
    Task<Schema> WaitForArrowChannelSchemaAsync(string branchAlias, CancellationToken ct = default);

    // --- Row-based Bridge Methods (Compatibility) ---
    void RegisterChannel(string branchAlias, Channel<IReadOnlyList<object?[]>> channel, IReadOnlyList<PipeColumnInfo> columns);
    void UpdateChannelColumns(string branchAlias, IReadOnlyList<PipeColumnInfo> columns);
    Task<IReadOnlyList<PipeColumnInfo>> WaitForChannelColumnsAsync(string branchAlias, CancellationToken ct = default);
    (Channel<IReadOnlyList<object?[]>> Channel, IReadOnlyList<PipeColumnInfo> Columns)? GetChannel(string branchAlias);

    // --- Common ---
    bool ContainsChannel(string branchAlias);
}
