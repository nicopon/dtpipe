using Apache.Arrow;
using System.Threading.Channels;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// Registry interface for Arrow (columnar) in-memory channels.
/// Used by processors such as DataFusion that operate exclusively on Arrow RecordBatches.
/// </summary>
public interface IArrowChannelRegistry
{
    /// <summary>Registers an Arrow RecordBatch channel for a branch alias.</summary>
    void RegisterArrowChannel(string branchAlias, Channel<RecordBatch> channel, Schema schema);

    /// <summary>Updates the schema of an Arrow channel once known at runtime.</summary>
    void UpdateArrowChannelSchema(string branchAlias, Schema schema);

    /// <summary>Retrieves the Arrow channel and its schema for a given alias. Returns null if not found.</summary>
    (Channel<RecordBatch> Channel, Schema Schema)? GetArrowChannel(string branchAlias);

    /// <summary>Waits asynchronously for the Arrow channel schema to be set, then returns it.</summary>
    Task<Schema> WaitForArrowChannelSchemaAsync(string branchAlias, CancellationToken ct = default);
}
