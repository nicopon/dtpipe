using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace DtPipe.Adapters.MemoryChannel;

/// <summary>
/// A stream reader that reads Arrow RecordBatches from an in-memory Arrow channel
/// and converts them to native object[] rows for downstream native consumers.
/// Enables native fan-out branches (e.g. CSV passthru) to consume Arrow channels
/// produced by columnar sources (parquet, Arrow files, DuckDB, generate, etc.).
/// </summary>
public sealed class ArrowMemoryChannelStreamReader : IStreamReader, IColumnarStreamReader
{
    private readonly ChannelReader<RecordBatch> _reader;
    private readonly IMemoryChannelRegistry _registry;
    private readonly string _alias;
    private IReadOnlyList<PipeColumnInfo>? _columns;

    public ArrowMemoryChannelStreamReader(ChannelReader<RecordBatch> reader, IMemoryChannelRegistry registry, string alias)
    {
        _reader = reader;
        _registry = registry;
        _alias = alias;
    }

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;
    public Schema? Schema { get; private set; }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        Schema = await _registry.WaitForArrowChannelSchemaAsync(_alias, ct);
        _columns = Schema.FieldsList
            .Select(f => new PipeColumnInfo(f.Name, ArrowTypeMapper.GetClrTypeFromField(f), f.IsNullable))
            .ToList();
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var batch in _reader.ReadAllAsync(ct))
        {
            yield return batch;
        }
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var batch in _reader.ReadAllAsync(ct))
        {
            var rows = new object?[batch.Length][];
            for (int i = 0; i < batch.Length; i++)
            {
                var row = new object?[batch.Schema.FieldsList.Count];
                for (int j = 0; j < row.Length; j++)
                    row[j] = ArrowTypeMapper.GetValueForField(batch.Column(j), batch.Schema.GetFieldByIndex(j), i);
                rows[i] = row;
            }
            yield return new ReadOnlyMemory<object?[]>(rows);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
