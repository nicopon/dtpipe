using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using System.Threading.Channels;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.Logging;

namespace DtPipe.Adapters.MemoryChannel;

/// <summary>
/// A specialized data writer that pushes batches of rows into an in-memory channel.
/// Used for orchestrating DAG branches where the output of one branch is the input of another.
/// </summary>
public class MemoryChannelDataWriter : IRowDataWriter
{
    private readonly ChannelWriter<IReadOnlyList<object?[]>> _writer;
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _alias;
    private readonly ILogger<MemoryChannelDataWriter> _logger;
    private bool _isDisposed;

    public MemoryChannelDataWriter(ChannelWriter<IReadOnlyList<object?[]>> writer, IMemoryChannelRegistry registry, string alias, ILogger<MemoryChannelDataWriter> logger)
    {
        _writer = writer;
        _registry = registry;
        _alias = alias;
        _logger = logger;
    }

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        _logger.LogDebug("MemoryChannelDataWriter '{Alias}' initialized with {ColumnCount} columns. Data buffered in RAM.", _alias, columns.Count);
        // Metadata is registered upfront by the orchestrator (empty), but now we dynamically update it.
        _registry.UpdateChannelColumns(_alias, columns);
        await Task.CompletedTask;
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_isDisposed)
        {
            throw new ObjectDisposedException(nameof(MemoryChannelDataWriter));
        }

        // Write the batch to the channel
        await _writer.WriteAsync(rows, ct);
    }

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        if (!_isDisposed)
        {
            _writer.TryComplete();
            _isDisposed = true;
        }
        await Task.CompletedTask;
    }

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await CompleteAsync();
    }
}
