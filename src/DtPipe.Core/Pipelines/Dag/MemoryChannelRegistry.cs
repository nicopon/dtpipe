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
            // If the existing Arrow schema is "richer" (has Structs/Lists) than the one
            // the factory would create from row columns (Map typeof(object) to String),
            // then we should PRESERVE the existing schema.
            var rowBasedSchema = ArrowSchemaFactory.Create(columns);
            bool existingIsRicher = arrowData.Schema.FieldsList.Any(f => f.DataType is StructType or ListType or MapType);
            
            if (!existingIsRicher)
            {
                _arrowChannels[branchAlias] = (arrowData.Channel, rowBasedSchema);
                if (_arrowSchemaTcs.TryGetValue(branchAlias, out var tcs))
                {
                    tcs.TrySetResult(rowBasedSchema);
                }
            }
            found = true;
        }

        // Propagate to fan-out sub-channels
        var fanPrefix = branchAlias + "__fan_";
        foreach (var key in _channels.Keys)
        {
            if (key.StartsWith(fanPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var fanData = _channels[key];
                _channels[key] = (fanData.Channel, columns);
                if (_columnTcs.TryGetValue(key, out var tcs)) tcs.TrySetResult(columns);
            }
        }
        PropagateArrowSchemaToFanOut(fanPrefix, ArrowSchemaFactory.Create(columns));

        if (!found)
        {
            throw new InvalidOperationException($"Cannot update columns: channel '{branchAlias}' is not registered.");
        }
    }

    private void PropagateArrowSchemaToFanOut(string fanPrefix, Schema schema)
    {
        foreach (var key in _arrowChannels.Keys)
        {
            if (!key.StartsWith(fanPrefix, StringComparison.OrdinalIgnoreCase)) continue;
            var fanData = _arrowChannels[key];
            _arrowChannels[key] = (fanData.Channel, schema);
            if (_arrowSchemaTcs.TryGetValue(key, out var tcs)) tcs.TrySetResult(schema);
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
        bool found = false;
        if (_arrowChannels.TryGetValue(branchAlias, out var arrowData))
        {
            // Protection: if existing schema is richer than the new one, preserve it.
            bool existingIsRicher = arrowData.Schema.FieldsList.Any(f => f.DataType is StructType or ListType or MapType);
            bool newIsRicher = schema.FieldsList.Any(f => f.DataType is StructType or ListType or MapType);
            
            if (newIsRicher || !existingIsRicher || arrowData.Schema.FieldsList.Count == 0)
            {
                _arrowChannels[branchAlias] = (arrowData.Channel, schema);
                if (_arrowSchemaTcs.TryGetValue(branchAlias, out var tcs))
                {
                    tcs.TrySetResult(schema);
                }
            }
            found = true;
        }
        
        // Propagate to fan-out sub-channels
        PropagateArrowSchemaToFanOut(branchAlias + "__fan_", schema);

        if (!found)
        {
            throw new InvalidOperationException($"Cannot update Arrow schema: channel '{branchAlias}' is not registered.");
        }
    }

    public (Channel<RecordBatch> Channel, Schema Schema)? GetArrowChannel(string alias)
    {
        if (_arrowChannels.TryGetValue(alias, out var tuple))
        {
            return tuple;
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
