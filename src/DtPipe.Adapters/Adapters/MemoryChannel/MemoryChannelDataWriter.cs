using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.Logging;
using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Infrastructure;
using System.Threading.Channels;

namespace DtPipe.Adapters.MemoryChannel;

/// <summary>
/// A specialized data writer that pushes batches of rows into an in-memory channel.
/// Used for orchestrating DAG branches where the output of one branch is the input of another.
/// </summary>
public class MemoryChannelDataWriter : IRowDataWriter
{
    private readonly ChannelWriter<RecordBatch> _writer;
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _alias;
    private readonly ILogger<MemoryChannelDataWriter> _logger;
    private readonly ArrowRowToColumnarBridge _bridge;
    private bool _isDisposed;
    private Task? _drainTask;

    public MemoryChannelDataWriter(ChannelWriter<RecordBatch> writer, IMemoryChannelRegistry registry, string alias, ILogger<MemoryChannelDataWriter> logger)
    {
        _writer = writer;
        _registry = registry;
        _alias = alias;
        _logger = logger;
        _bridge = new ArrowRowToColumnarBridge(logger);
    }

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        _logger.LogDebug("MemoryChannelDataWriter '{Alias}' initialized with {ColumnCount} columns. Data buffered in RAM via Arrow bridge.", _alias, columns.Count);
        
        // Metadata is registered upfront by the orchestrator (empty), but now we dynamically update it.
        _registry.UpdateChannelColumns(_alias, columns);

        // Initialize the bridge (batch size 1024 as per plan)
        await _bridge.InitializeAsync(columns, 1024, ct: ct);

        // Start background task to drain the bridge into the actual channel writer
        _drainTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in _bridge.ReadRecordBatchesAsync(CancellationToken.None))
                {
                    await _writer.WriteAsync(batch, CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error draining Arrow bridge for '{Alias}'", _alias);
                _writer.TryComplete(ex);
            }
        });
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_isDisposed)
        {
            throw new ObjectDisposedException(nameof(MemoryChannelDataWriter));
        }

        // Bridge expects Memory<object?[]>. Convert if necessary.
        object? [] [] array;
        if (rows is object? [][] direct)
        {
            array = direct;
        }
        else
        {
            array = rows.ToArray();
        }

        await _bridge.IngestRowsAsync(array, ct);
    }

    public async ValueTask CompleteAsync(CancellationToken ct = default)
    {
        if (!_isDisposed)
        {
            await _bridge.CompleteAsync(ct);
            if (_drainTask != null)
            {
                await _drainTask;
            }
            _writer.TryComplete();
            _isDisposed = true;
        }
    }

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        throw new NotSupportedException("Executing raw commands is not supported for memory channels.");
    }

    public async ValueTask DisposeAsync()
    {
        await CompleteAsync();
    }
}
