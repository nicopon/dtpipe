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
/// Standardized on Apache Arrow IPC for all inter-branch communication.
/// </summary>
public class MemoryChannelRegistry : IMemoryChannelRegistry
{
    private static readonly object _schemaLock = new();

    private readonly ConcurrentDictionary<string, (Channel<RecordBatch> Channel, Schema Schema)> _arrowChannels
        = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, TaskCompletionSource<Schema>> _arrowSchemaTcs
        = new(StringComparer.OrdinalIgnoreCase);

    public void UpdateChannelColumns(string branchAlias, IReadOnlyList<PipeColumnInfo> columns)
    {
        // Forward row-based column updates to the Arrow schema storage
        UpdateArrowChannelSchema(branchAlias, ArrowSchemaFactory.Create(columns));
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
        // Map the Arrow schema back to row-based PipeColumnInfo for legacy components
        var schema = await WaitForArrowChannelSchemaAsync(branchAlias, ct);
        return ArrowSchemaFactory.ToPipeColumns(schema);
    }

    public bool ContainsChannel(string branchAlias)
    {
        return _arrowChannels.ContainsKey(branchAlias);
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
            bool existingIsRicher = ArrowSchemaFactory.IsRichSchema(arrowData.Schema);
            bool newIsRicher = ArrowSchemaFactory.IsRichSchema(schema);
            
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
