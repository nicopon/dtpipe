using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// A thread-safe registry for sharing in-memory channels between pipeline branches.
/// </summary>
public class MemoryChannelRegistry : IMemoryChannelRegistry
{
    private readonly ConcurrentDictionary<string, (Channel<IReadOnlyList<object?[]>> Channel, IReadOnlyList<PipeColumnInfo> Columns)> _channels
        = new(StringComparer.OrdinalIgnoreCase);
    private static readonly object _schemaLock = new();

    private readonly ConcurrentDictionary<string, TaskCompletionSource<IReadOnlyList<PipeColumnInfo>>> _columnTcs
        = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, (Channel<RecordBatch> Channel, Schema Schema)> _arrowChannels
        = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, TaskCompletionSource<Schema>> _arrowSchemaTcs
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
        bool found = false;
        if (_channels.TryGetValue(branchAlias, out var channelData))
        {
            _channels[branchAlias] = (channelData.Channel, columns);
            if (_columnTcs.TryGetValue(branchAlias, out var tcs))
            {
                tcs.TrySetResult(columns);
            }
            found = true;
        }

        if (_arrowChannels.TryGetValue(branchAlias, out var arrowData))
        {
            var schema = BuildArrowSchema(columns);
            _arrowChannels[branchAlias] = (arrowData.Channel, schema);
            if (_arrowSchemaTcs.TryGetValue(branchAlias, out var tcs))
            {
                tcs.TrySetResult(schema);
            }
            found = true;
        }

        if (!found)
        {
            throw new InvalidOperationException($"Cannot update columns: channel '{branchAlias}' is not registered.");
        }
    }

    private Schema BuildArrowSchema(IReadOnlyList<PipeColumnInfo> columns)
    {
        lock (_schemaLock)
        {
            var builder = new Schema.Builder();
            foreach (var col in columns)
            {
                builder.Field(new Field(col.Name, ArrowTypeMapper.GetArrowType(col.ClrType), col.IsNullable));
            }
            return builder.Build();
        }
    }

    private static IArrowType GetArrowType(Type type) => ArrowTypeMapper.GetArrowType(type);

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
        return _channels.ContainsKey(branchAlias) || _arrowChannels.ContainsKey(branchAlias);
    }

    public void RegisterArrowChannel(string branchAlias, Channel<RecordBatch> channel, Schema schema)
    {
        _arrowSchemaTcs.TryAdd(branchAlias, new TaskCompletionSource<Schema>(TaskCreationOptions.RunContinuationsAsynchronously));
        if (!_arrowChannels.TryAdd(branchAlias, (channel, schema)))
        {
            throw new InvalidOperationException($"An Arrow channel with the alias '{branchAlias}' is already registered.");
        }
    }

    public void UpdateArrowChannelSchema(string branchAlias, Schema schema)
    {
        if (_arrowChannels.TryGetValue(branchAlias, out var channelData))
        {
            _arrowChannels[branchAlias] = (channelData.Channel, schema);
            if (_arrowSchemaTcs.TryGetValue(branchAlias, out var tcs))
            {
                tcs.TrySetResult(schema);
            }
        }
        else
        {
            throw new InvalidOperationException($"Cannot update Arrow schema: channel '{branchAlias}' is not registered.");
        }
    }

    public (Channel<RecordBatch> Channel, Schema Schema)? GetArrowChannel(string branchAlias)
    {
        if (_arrowChannels.TryGetValue(branchAlias, out var channelData))
        {
            return channelData;
        }
        return null;
    }

    public async Task<Schema> WaitForArrowChannelSchemaAsync(string branchAlias, CancellationToken ct = default)
    {
        if (_arrowSchemaTcs.TryGetValue(branchAlias, out var tcs))
        {
            using var reg = ct.Register(() => tcs.TrySetCanceled(ct));
            return await tcs.Task;
        }
        throw new InvalidOperationException($"An Arrow channel with the alias '{branchAlias}' is not registered.");
    }
}
