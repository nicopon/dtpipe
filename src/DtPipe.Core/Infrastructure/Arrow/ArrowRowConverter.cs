using Apache.Arrow;
using DtPipe.Core.Models;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Core.Infrastructure.Arrow;

/// <summary>
/// Centralized utility for synchronous conversion between C# rows and Apache Arrow RecordBatches.
/// This ensures 100% behavioral conformity between streaming bridges and row-mode fallbacks.
/// </summary>
public static class ArrowRowConverter
{
    /// <summary>
    /// Creates a RecordBatch from a collection of rows.
    /// </summary>
    public static RecordBatch ToRecordBatch(Schema schema, IEnumerable<IReadOnlyList<object?>> rows, int count)
    {
        var builders = schema.FieldsList.Select(f => ArrowTypeMapper.CreateBuilder(f.DataType)).ToList();
        
        foreach (var row in rows)
        {
            for (int i = 0; i < builders.Count; i++)
            {
                ArrowTypeMapper.AppendValue(builders[i], row[i]);
            }
        }

        return BuildBatch(schema, builders, count);
    }

    /// <summary>
    /// Creates a RecordBatch from a set of builders.
    /// </summary>
    public static RecordBatch BuildBatch(Schema schema, IReadOnlyList<IArrowArrayBuilder> builders, int count)
    {
        var arrays = builders.Select(ArrowTypeMapper.BuildArray).ToList();
        return new RecordBatch(schema, arrays, count);
    }

    /// <summary>
    /// Extracts a single row from a RecordBatch at a specific index.
    /// </summary>
    public static object?[] ToRow(RecordBatch batch, int index)
    {
        var row = new object?[batch.Schema.FieldsList.Count];
        for (int i = 0; i < row.Length; i++)
        {
            row[i] = ArrowTypeMapper.GetValueForField(batch.Column(i), batch.Schema.GetFieldByIndex(i), index);
        }
        return row;
    }

    /// <summary>
    /// Flattens a RecordBatch into chunks of ReadOnlyMemory row arrays of a requested size.
    /// </summary>
    public static System.Collections.Generic.IEnumerable<System.ReadOnlyMemory<object?[]>> FlattenBatch(RecordBatch batch, int requestedBatchSize)
    {
        var rowCount = batch.Length;
        var flatBatch = new object?[requestedBatchSize][];
        var currentIndex = 0;

        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++)
        {
            flatBatch[currentIndex++] = ToRow(batch, rowIdx);

            if (currentIndex >= requestedBatchSize)
            {
                yield return new System.ReadOnlyMemory<object?[]>(flatBatch, 0, currentIndex);
                flatBatch = new object?[requestedBatchSize][];
                currentIndex = 0;
            }
        }

        if (currentIndex > 0)
        {
            yield return new System.ReadOnlyMemory<object?[]>(flatBatch, 0, currentIndex);
        }
    }
}
