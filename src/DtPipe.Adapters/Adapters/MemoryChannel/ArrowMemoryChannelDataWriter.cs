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
        // We still need to notify the registry about the schema,
        // but we don't need builders anymore.
        // We can't build the Schema easily without the GetArrowType mapping,
        // which we moved to the Bridge.
        // However, DuckDB needs the schema to be known in advance for WaitForArrowChannelSchemaAsync.

        // Let's keep a simplified BuildSchema here for registry notification,
        // OR we could make ArrowRowToColumnarBridge responsible for this if it's aware of the registry.
        // But the writer is the one usually owning the registration.

        var schema = BuildSchema(columns);
        _registry.UpdateArrowChannelSchema(_alias, schema);

        _logger.LogInformation("Arrow channel '{Alias}' initialized (Pure Columnar).", _alias);
        return ValueTask.CompletedTask;
    }

    public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        await _writer.WriteAsync(batch, ct);
    }

    public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        throw new NotSupportedException("ArrowMemoryChannelDataWriter is purely columnar. Use IColumnarDataWriter.WriteRecordBatchAsync instead.");
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

    private IArrowType GetArrowType(Type type) => ArrowTypeMapper.GetLogicalType(type).ArrowType;
}
