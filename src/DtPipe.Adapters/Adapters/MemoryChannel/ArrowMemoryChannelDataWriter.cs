using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace DtPipe.Adapters.MemoryChannel;

/// <summary>
/// An IDataWriter that pushes RecordBatches into a Channel&lt;RecordBatch&gt;
/// for downstream Arrow consumers.
/// This writer is now purely columnar.
/// </summary>
public sealed class ArrowMemoryChannelDataWriter : IColumnarDataWriter
{
    private readonly ChannelWriter<RecordBatch> _writer;
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _alias;
    private readonly ILogger<ArrowMemoryChannelDataWriter> _logger;


    public ArrowMemoryChannelDataWriter(
        ChannelWriter<RecordBatch> writer,
        IMemoryChannelRegistry registry,
        string alias,
        int batchSize, // Kept for signature compatibility but unused
        ILogger<ArrowMemoryChannelDataWriter> logger)
    {
        _writer = writer;
        _registry = registry;
        _alias = alias;
        _logger = logger;
    }

    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        // Only publish the schema if the registry doesn't already have a richer one.
        // Columnar readers (e.g. JsonLStreamReader) publish the correct Arrow schema — including
        // StructType / ListType fields — during OpenAsync. Publishing a flat PipeColumnInfo-derived
        // schema first would resolve the channel's TaskCompletionSource with the wrong schema, which
        // downstream consumers cannot undo even if the reader publishes the rich schema later.
        var existing = _registry.GetArrowChannel(_alias);
        bool existingIsRicher = existing != null && ArrowSchemaFactory.IsRichSchema(existing.Value.Schema);

        if (!existingIsRicher)
        {
            _registry.UpdateArrowChannelSchema(_alias, BuildSchema(columns));
        }

        _logger.LogInformation("Arrow channel '{Alias}' initialized (Pure Columnar).", _alias);
        return ValueTask.CompletedTask;
    }

    public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        await _writer.WriteAsync(batch, ct);
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        _writer.TryComplete();
        _logger.LogInformation("Arrow channel '{Alias}' marked as complete.", _alias);
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        throw new NotSupportedException("Executing raw commands is not supported for memory channels.");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private Schema BuildSchema(IReadOnlyList<PipeColumnInfo> columns)
    {
        var builder = new Schema.Builder();
        foreach (var col in columns)
        {
            builder.Field(ArrowTypeMapper.GetField(col.Name, col.ClrType, col.IsNullable));
        }
        return builder.Build();
    }

}
