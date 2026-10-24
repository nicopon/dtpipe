using System.Threading.Channels;
using Apache.Arrow;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// A unified registry for sharing in-memory channels between pipeline branches.
/// Standardized on Apache Arrow RecordBatches for the wire protocol.
/// </summary>
public interface IMemoryChannelRegistry
{
    void RegisterArrowChannel(string branchAlias, Channel<RecordBatch> channel, Schema schema);
    void UpdateArrowChannelSchema(string branchAlias, Schema schema);
    (Channel<RecordBatch> Channel, Schema Schema)? GetArrowChannel(string branchAlias);
    Task<Schema> WaitForArrowChannelSchemaAsync(string branchAlias, CancellationToken ct = default);
    bool ContainsChannel(string branchAlias);
    void Clear();
}
