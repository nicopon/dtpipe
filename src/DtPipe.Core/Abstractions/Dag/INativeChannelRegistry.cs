using DtPipe.Core.Models;
using System.Threading.Channels;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// Registry interface for native (row-based) in-memory channels.
/// </summary>
public interface INativeChannelRegistry
{
    /// <summary>Registers a new native memory channel for a given branch alias.</summary>
    void RegisterChannel(string branchAlias, Channel<IReadOnlyList<object?[]>> channel, IReadOnlyList<PipeColumnInfo> columns);

    /// <summary>Updates the schema metadata for an already registered native channel.</summary>
    void UpdateChannelColumns(string branchAlias, IReadOnlyList<PipeColumnInfo> columns);

    /// <summary>Waits asynchronously for the specified branch to update its schema.</summary>
    Task<IReadOnlyList<PipeColumnInfo>> WaitForChannelColumnsAsync(string branchAlias, CancellationToken ct = default);

    /// <summary>Retrieves the native memory channel and metadata for the specified alias. Returns null if not found.</summary>
    (Channel<IReadOnlyList<object?[]>> Channel, IReadOnlyList<PipeColumnInfo> Columns)? GetChannel(string branchAlias);

    /// <summary>Checks if a channel (native or Arrow) exists for the given alias.</summary>
    bool ContainsChannel(string branchAlias);
}
