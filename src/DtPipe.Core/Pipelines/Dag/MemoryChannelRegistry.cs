using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// A thread-safe registry for sharing in-memory channels between pipeline branches.
/// </summary>
public class MemoryChannelRegistry : IMemoryChannelRegistry
{
    private readonly ConcurrentDictionary<string, (Channel<IReadOnlyList<object?[]>> Channel, IReadOnlyList<PipeColumnInfo> Columns)> _channels
        = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, TaskCompletionSource<IReadOnlyList<PipeColumnInfo>>> _columnTcs
        = new(StringComparer.OrdinalIgnoreCase);

    public void RegisterChannel(string branchAlias, Channel<IReadOnlyList<object?[]>> channel, IReadOnlyList<PipeColumnInfo> columns)
    {
        _columnTcs.TryAdd(branchAlias, new TaskCompletionSource<IReadOnlyList<PipeColumnInfo>>(TaskCreationOptions.RunContinuationsAsynchronously));
        if (!_channels.TryAdd(branchAlias, (channel, columns)))
        {
            throw new InvalidOperationException($"A channel with the alias '{branchAlias}' is already registered.");
        }
    }

    public void UpdateChannelColumns(string branchAlias, IReadOnlyList<PipeColumnInfo> columns)
    {
        if (_channels.TryGetValue(branchAlias, out var channelData))
        {
            _channels[branchAlias] = (channelData.Channel, columns);
            if (_columnTcs.TryGetValue(branchAlias, out var tcs))
            {
                tcs.TrySetResult(columns);
            }
        }
        else
        {
            throw new InvalidOperationException($"Cannot update columns: channel '{branchAlias}' is not registered.");
        }
    }

    public async Task<IReadOnlyList<PipeColumnInfo>> WaitForChannelColumnsAsync(string branchAlias, CancellationToken ct = default)
    {
        if (_columnTcs.TryGetValue(branchAlias, out var tcs))
        {
            using var reg = ct.Register(() => tcs.TrySetCanceled(ct));
            return await tcs.Task;
        }
        throw new InvalidOperationException($"A channel with the alias '{branchAlias}' is not registered.");
    }

    public (Channel<IReadOnlyList<object?[]>> Channel, IReadOnlyList<PipeColumnInfo> Columns)? GetChannel(string branchAlias)
    {
        if (_channels.TryGetValue(branchAlias, out var channelData))
        {
            return channelData;
        }
        return null;
    }

    public bool ContainsChannel(string branchAlias)
    {
        return _channels.ContainsKey(branchAlias);
    }
}
