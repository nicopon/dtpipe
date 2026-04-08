using Apache.Arrow;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Tests;

/// <summary>
/// Test helper: builds RecordBatches from object?[] rows and PipeColumnInfo schema,
/// and extracts values from result batches via ArrowTypeMapper.
/// </summary>
internal static class TestBatchBuilder
{
    /// <summary>
    /// Creates a RecordBatch from a schema and a set of rows.
    /// Each row must have the same column count as the schema.
    /// </summary>
    internal static RecordBatch FromRows(IReadOnlyList<PipeColumnInfo> columns, params object?[][] rows)
    {
        var fields = new List<Field>(columns.Count);
        var builders = new List<IArrowArrayBuilder>(columns.Count);

        foreach (var col in columns)
        {
            fields.Add(ArrowTypeMapper.GetField(col.Name, col.ClrType, col.IsNullable));
            builders.Add(ArrowTypeMapper.CreateBuilder(ArrowTypeMapper.GetLogicalType(col.ClrType).ArrowType));
        }

        foreach (var row in rows)
            for (int i = 0; i < columns.Count; i++)
                ArrowTypeMapper.AppendValue(builders[i], row[i]);

        var arrays = builders.Select(ArrowTypeMapper.BuildArray).ToArray();
        return new RecordBatch(new Schema(fields, null), arrays, rows.Length);
    }

    /// <summary>
    /// Extracts a value from a RecordBatch, resolving Arrow extension types (e.g. arrow.uuid → Guid).
    /// </summary>
    internal static object? GetVal(RecordBatch batch, int colIdx, int rowIdx)
        => ArrowTypeMapper.GetValueForField(batch.Column(colIdx), batch.Schema.GetFieldByIndex(colIdx), rowIdx);
}
