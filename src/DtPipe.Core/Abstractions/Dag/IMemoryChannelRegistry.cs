using DtPipe.Core.Models;
using System.Threading.Channels;

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
}
