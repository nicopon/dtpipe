using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;

namespace DtPipe.Core.Infrastructure.Arrow;

public class ArrowColumnarToRowBridge : IColumnarToRowBridge
{
#pragma warning disable CS1998 // Async method lacks 'await' — required for IAsyncEnumerable iterator
    private Schema? _lastSchema;
    private Dictionary<string, int>? _nameToIndex;

    public async IAsyncEnumerable<IReadOnlyList<object?>> ConvertBatchToRowsAsync(RecordBatch batch, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_lastSchema != batch.Schema)
        {
            _lastSchema = batch.Schema;
            _nameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
                _nameToIndex[batch.Schema.GetFieldByIndex(i).Name] = i;
        }

        int rowCount = batch.Length;

        for (int i = 0; i < rowCount; i++)
        {
            if (ct.IsCancellationRequested) break;
            yield return new ArrowRowView(batch, i, _nameToIndex!);
        }
    }
#pragma warning restore CS1998

    public static object? GetValue(IArrowArray array, int index)
        => ArrowTypeMapper.GetValue(array, index);
}
