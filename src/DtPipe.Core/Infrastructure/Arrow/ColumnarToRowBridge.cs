using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;

namespace DtPipe.Core.Infrastructure.Arrow;

public class ArrowColumnarToRowBridge : IColumnarToRowBridge
{
    public async IAsyncEnumerable<object?[]> ConvertBatchToRowsAsync(RecordBatch batch, [EnumeratorCancellation] CancellationToken ct = default)
    {
        int rowCount = batch.Length;
        int colCount = batch.Schema.FieldsList.Count;

        for (int i = 0; i < rowCount; i++)
        {
            if (ct.IsCancellationRequested) break;

            var row = new object?[colCount];
            for (int j = 0; j < colCount; j++)
            {
                row[j] = GetValue(batch.Column(j), i);
            }
            yield return row;
        }

        await Task.CompletedTask;
    }

    public static object? GetValue(IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        return array switch
        {
            StringArray a => a.GetString(index),
            Int32Array a => (long)a.GetValue(index)!,
            Int64Array a => a.GetValue(index),
            DoubleArray a => a.GetValue(index),
            FloatArray a => (double)a.GetValue(index)!,
            BooleanArray a => a.GetValue(index),
            Date64Array a => a.GetDateTime(index),
            TimestampArray a => a.GetTimestamp(index),
            _ => null
        };
    }
}
