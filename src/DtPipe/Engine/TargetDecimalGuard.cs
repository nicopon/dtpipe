using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;

namespace DtPipe.Engine;

/// <summary>
/// Stands in front of a writer whose target already exists and refuses a decimal with more decimals
/// than the column it goes to keeps. Every engine rounds such a value to its column's scale, or reports
/// it where a bulk load does not listen, and the run ends in code 0 with another number in the table.
/// Whether a value fits is a question of the value, so the guard reads the cell; it converts nothing.
/// One point in front of all writers, where a check in each would be a copy per engine.
/// <para>
/// A column the source cannot hold anything finer in costs nothing, and a run with no such column has no
/// guard at all. The shape mirrors the writer's capabilities, which the engine routes by.
/// </para>
/// </summary>
internal static class TargetDecimalGuard
{
    private sealed record GuardedColumn(int Ordinal, string Name, string TargetType, int Scale);

    /// <summary>
    /// <paramref name="writer"/> behind a guard, or itself when no column of <paramref name="report"/> needs one.
    /// A report is null when the target was not inspected, which <c>--no-schema-validation</c> asks for.
    /// </summary>
    public static IDataWriter Wrap(IDataWriter writer, SchemaCompatibilityReport? report, IReadOnlyList<PipeColumnInfo> columns)
    {
        var guarded = report is null ? [] : ColumnsAtRisk(report, columns);
        if (guarded.Length == 0) return writer;

        return writer switch
        {
            IColumnarDataWriter and IRowDataWriter => new EitherShape(writer, guarded),
            IColumnarDataWriter columnar => new Columnar(columnar, guarded),
            IRowDataWriter row => new Row(row, guarded),
            _ => writer
        };
    }

    private static GuardedColumn[] ColumnsAtRisk(SchemaCompatibilityReport report, IReadOnlyList<PipeColumnInfo> columns)
    {
        var guarded = new List<GuardedColumn>();
        foreach (var match in report.Columns)
        {
            if (match is not { SourceColumn: { } source, TargetColumn: { Scale: int keeps and >= 0 } target }) continue;
            if ((Nullable.GetUnderlyingType(source.ClrType) ?? source.ClrType) != typeof(decimal)) continue;
            if (!IsExactNumber(target.InferredClrType)) continue;

            // The most decimals the source can bring: nothing to read when the column keeps them all.
            if (DecimalWidth.Of(source.Precision, source.Scale).Scale <= keeps) continue;

            var ordinal = IndexOf(columns, source);
            if (ordinal >= 0) guarded.Add(new GuardedColumn(ordinal, source.Name, target.NativeType, keeps));
        }
        return guarded.ToArray();
    }

    private static int IndexOf(IReadOnlyList<PipeColumnInfo> columns, PipeColumnInfo column)
    {
        for (var i = 0; i < columns.Count; i++)
        {
            if (columns[i].Name == column.Name) return i;
        }
        return -1;
    }

    // A float column has no scale to round to, whatever its catalog reports for one.
    private static bool IsExactNumber(Type? type)
        => type == typeof(decimal) || type == typeof(long) || type == typeof(int) || type == typeof(short)
           || type == typeof(sbyte) || type == typeof(byte) || type == typeof(ushort) || type == typeof(uint)
           || type == typeof(ulong);

    private abstract class Guard(IDataWriter inner, GuardedColumn[] columns) : IDataWriter
    {
        private long _checked;

        protected void Check(IReadOnlyList<object?[]> rows)
        {
            for (var r = 0; r < rows.Count; r++)
            {
                foreach (var column in columns)
                {
                    if (rows[r][column.Ordinal] is decimal value && !DecimalWidth.Keeps(value, column.Scale))
                        throw Refusal(column, _checked + r + 1);
                }
            }
            _checked += rows.Count;
        }

        protected void Check(RecordBatch batch)
        {
            foreach (var column in columns)
            {
                var array = batch.Column(column.Ordinal);
                for (var r = 0; r < batch.Length; r++)
                {
                    if (ArrowTypeMapper.GetValue(array, r) is decimal value && !DecimalWidth.Keeps(value, column.Scale))
                        throw Refusal(column, _checked + r + 1);
                }
            }
            _checked += batch.Length;
        }

        private static ColumnValueRefusedException Refusal(GuardedColumn column, long record) => new(
            $"Record {record}", column.Name, $"written to {column.TargetType}",
            "holds a value with more decimals than the target column keeps",
            "Round it in a SQL step, or widen the target column.");

        public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> schema, CancellationToken ct = default) => inner.InitializeAsync(schema, ct);
        public ValueTask CompleteAsync(CancellationToken ct = default) => inner.CompleteAsync(ct);
        public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default) => inner.ExecuteCommandAsync(command, ct);
        public ValueTask DisposeAsync() => inner.DisposeAsync();

        // The batch is the guard's until the writer takes it: refused, nobody else disposes it.
        protected ValueTask WriteChecked(IColumnarDataWriter writer, RecordBatch batch, CancellationToken ct)
        {
            try { Check(batch); }
            catch { batch.Dispose(); throw; }
            return writer.WriteRecordBatchAsync(batch, ct);
        }

        protected ValueTask WriteChecked(IRowDataWriter writer, IReadOnlyList<object?[]> rows, CancellationToken ct)
        {
            Check(rows);
            return writer.WriteBatchAsync(rows, ct);
        }
    }

    private sealed class Row : Guard, IRowDataWriter
    {
        private readonly IRowDataWriter _writer;
        public Row(IRowDataWriter writer, GuardedColumn[] columns) : base(writer, columns) => _writer = writer;
        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) => WriteChecked(_writer, rows, ct);
    }

    private sealed class Columnar : Guard, IColumnarDataWriter
    {
        private readonly IColumnarDataWriter _writer;
        public Columnar(IColumnarDataWriter writer, GuardedColumn[] columns) : base(writer, columns) => _writer = writer;
        public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default) => WriteChecked(_writer, batch, ct);
    }

    private sealed class EitherShape : Guard, IRowDataWriter, IColumnarDataWriter
    {
        private readonly IDataWriter _writer;
        public EitherShape(IDataWriter writer, GuardedColumn[] columns) : base(writer, columns) => _writer = writer;
        public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default) => WriteChecked((IRowDataWriter)_writer, rows, ct);
        public ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default) => WriteChecked((IColumnarDataWriter)_writer, batch, ct);
    }
}
