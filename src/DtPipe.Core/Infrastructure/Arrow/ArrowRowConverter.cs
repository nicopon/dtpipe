using Apache.Arrow;
using Apache.Arrow.Serialization;
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
    /// <param name="schema">The batch's schema.</param>
    /// <param name="rows">The rows, in order.</param>
    /// <param name="count">How many rows there are.</param>
    /// <param name="firstRecord">
    /// One-based position of the first row in the stream, for the fault of a value a column cannot take;
    /// null when the caller does not count them.
    /// </param>
    public static RecordBatch ToRecordBatch(
        Schema schema, IEnumerable<IReadOnlyList<object?>> rows, int count, long? firstRecord = null)
    {
        var builders = schema.FieldsList.Select(f => ArrowTypeMapper.CreateBuilder(f.DataType)).ToList();
        var appenders = builders.Select(ArrowTypeMapper.ResolveAppender).ToArray();

        long position = 0;
        int i = 0;
        try
        {
            foreach (var row in rows)
            {
                for (i = 0; i < appenders.Length; i++)
                {
                    appenders[i](row[i]);
                }
                position++;
            }
        }
        catch (Exception fault) when (ArrowValueException.IsValueFault(fault))
        {
            var field = schema.GetFieldByIndex(i);
            throw new ArrowValueException(field.Name, field.DataType, fault.GetType().Name, firstRecord + position);
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

    private static object?[] ToRow(Func<int, object?>[] readers, int index)
    {
        var row = new object?[readers.Length];
        for (int i = 0; i < row.Length; i++)
        {
            row[i] = readers[i](index);
        }
        return row;
    }

    /// <summary>
    /// Flattens a RecordBatch into chunks of ReadOnlyMemory row arrays of a requested size.
    /// </summary>
    public static System.Collections.Generic.IEnumerable<System.ReadOnlyMemory<object?[]>> FlattenBatch(RecordBatch batch, int requestedBatchSize)
    {
        var rowCount = batch.Length;
        // Compiled once for the batch: every row reads through the same readers.
        var readers = ArrowTypeMapper.CompileValueReaders(batch);
        var flatBatch = new object?[requestedBatchSize][];
        var currentIndex = 0;

        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++)
        {
            flatBatch[currentIndex++] = ToRow(readers, rowIdx);

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
