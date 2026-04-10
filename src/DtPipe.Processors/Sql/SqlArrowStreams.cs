using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.Sql;

/// <summary>
/// Wraps a ChannelReader as an IArrowArrayStream for streaming FFI bridging.
/// </summary>
internal sealed class ChannelArrowStream : IArrowArrayStream
{
    private readonly Schema _schema;
    private readonly ChannelReader<RecordBatch> _reader;
    private readonly ILogger _logger;
    private readonly CancellationToken _ct;

    public ChannelArrowStream(Schema schema, ChannelReader<RecordBatch> reader, ILogger logger, CancellationToken ct)
    {
        _schema = schema;
        _reader = reader;
        _logger = logger;
        _ct = ct;
    }

    public Schema Schema => _schema;

    public async ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_ct, cancellationToken);
            if (await _reader.WaitToReadAsync(linkedCts.Token).ConfigureAwait(false) && _reader.TryRead(out var batch))
            {
                return batch;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) 
        { 
            _logger.LogError(ex, "ChannelArrowStream Error: {Message}", ex.Message); 
            throw; 
        }
        return null;
    }

    public void Dispose() { }
}
