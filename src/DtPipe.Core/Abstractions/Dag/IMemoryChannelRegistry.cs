using DtPipe.Core.Models;
using System.Threading.Channels;
using Apache.Arrow;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// A registry for sharing in-memory channels between pipeline branches.
/// Used by the orchestrator to register output channels, and by downstream
/// readers (XStreamers) to consume them.
/// </summary>
public interface IMemoryChannelRegistry
{
    /// <summary>
    /// Registers a new memory channel for a given branch alias.
    /// </summary>
    void RegisterChannel(string branchAlias, Channel<IReadOnlyList<object?[]>> channel, IReadOnlyList<PipeColumnInfo> columns);

    /// <summary>
    /// Updates the schema metadata for an already registered channel.
    /// Used by MemoryChannelDataWriter once the read schema is dynamically detected.
    /// </summary>
    void UpdateChannelColumns(string branchAlias, IReadOnlyList<PipeColumnInfo> columns);

    /// <summary>
    /// Asynchronously waits for the specified branch to update its schema.
    /// Used by downstream XStreamers to block during OpenAsync until dependencies are ready.
    /// </summary>
    Task<IReadOnlyList<PipeColumnInfo>> WaitForChannelColumnsAsync(string branchAlias, CancellationToken ct = default);

    /// <summary>
    /// Retrieves the memory channel and metadata associated with the specified branch alias.
    /// Returns null if not found.
    /// </summary>
    (Channel<IReadOnlyList<object?[]>> Channel, IReadOnlyList<PipeColumnInfo> Columns)? GetChannel(string branchAlias);

    /// <summary>
    /// Checks if a channel exists for the given alias.
    /// </summary>
    bool ContainsChannel(string branchAlias);

    /// <summary>Registers an Arrow RecordBatch channel for a branch alias.</summary>
    void RegisterArrowChannel(string branchAlias, Channel<RecordBatch> channel, Schema schema);

    /// <summary>Updates the schema of an Arrow channel once known at runtime.</summary>
    void UpdateArrowChannelSchema(string branchAlias, Schema schema);

    /// <summary>Retrieves the Arrow channel and its schema for a given alias. Returns null if not found.</summary>
    (Channel<RecordBatch> Channel, Schema Schema)? GetArrowChannel(string branchAlias);

    /// <summary>Waits asynchronously for the Arrow channel schema to be set, then returns it.</summary>
    Task<Schema> WaitForArrowChannelSchemaAsync(string branchAlias, CancellationToken ct = default);
}
