using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;

namespace DtPipe.Core.Infrastructure.Arrow;

public class ArrowColumnarToRowBridge : IColumnarToRowBridge
{
#pragma warning disable CS1998 // Async method lacks 'await' — required for IAsyncEnumerable iterator
    public async IAsyncEnumerable<object?[]> ConvertBatchToRowsAsync(RecordBatch batch, [EnumeratorCancellation] CancellationToken ct = default)
    {
        int rowCount = batch.Length;
        int colCount = batch.Schema.FieldsList.Count;

        for (int i = 0; i < rowCount; i++)
        {
            if (ct.IsCancellationRequested) break;

            var row = new object?[colCount];
            for (int j = 0; j < colCount; j++)
                row[j] = ArrowTypeMapper.GetValueForField(
                    batch.Column(j), batch.Schema.GetFieldByIndex(j), i);

            yield return row;
        }
    }
#pragma warning restore CS1998

    public static object? GetValue(IArrowArray array, int index)
        => ArrowTypeMapper.GetValue(array, index);
}
