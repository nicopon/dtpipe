using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization.Mapping;
using System.Linq;

namespace Apache.Arrow.Ado;

/// <summary>
/// A <see cref="DbDataReader"/> backed by a single Apache Arrow <see cref="RecordBatch"/>.
///
/// Primary use case: pass to <c>SqlBulkCopy.WriteToServerAsync(IDataReader)</c> to bulk-load
/// Arrow data into SQL Server without materialising an intermediate <c>DataTable</c>.
///
/// Also usable with any framework that accepts <see cref="IDataReader"/> (Oracle ODP.NET
/// <c>OracleBulkCopy</c>, NPGSQL COPY, etc.).
///
/// Symmetric counterpart to <see cref="AdoToArrow"/> (which converts in the opposite direction).
/// </summary>
public sealed class RecordBatchDataReader : DbDataReader
{
    private readonly RecordBatch _batch;
    private readonly Schema _schema;
    private readonly IArrowArray[] _columns;
    private readonly Field[] _fields;
    private readonly Type[] _fieldTypes;
    private readonly Func<int, object>[] _getters;
    private readonly BinaryArray?[] _bytes;
    private readonly int _fieldCount;
    private int _row = -1;
    private bool _closed;

    /// <summary>Creates a reader over the given batch. The batch is NOT disposed by this reader.</summary>
    public RecordBatchDataReader(RecordBatch batch)
    {
        _batch = batch ?? throw new ArgumentNullException(nameof(batch));
        _schema = batch.Schema;
        _fieldCount = _schema.FieldsList.Count;
        _columns = new IArrowArray[_fieldCount];
        _fields = new Field[_fieldCount];
        _fieldTypes = new Type[_fieldCount];
        _getters = new Func<int, object>[_fieldCount];
        _bytes = new BinaryArray?[_fieldCount];

        for (int i = 0; i < _fieldCount; i++)
        {
            var col = _batch.Column(i);
            var field = _schema.FieldsList[i];
            _columns[i] = col;
            _fields[i] = field;
            _fieldTypes[i] = ArrowTypeMap.GetClrTypeFromField(field);
            _getters[i] = BuildGetter(col, field);
            // The column whose getter copies a BinaryArray's bytes out: GetBytes reads them in place.
            _bytes[i] = !IsExtension(field) && col is BinaryArray b and not StringArray ? b : null;
        }
    }

    private static bool IsExtension(Field field)
        => field.HasMetadata && (field.Metadata.ContainsKey("ARROW:extension:name") || field.Metadata.ContainsKey("arrow.uuid"));

    private static Func<int, object> BuildGetter(IArrowArray array, Field field)
    {
        if (IsExtension(field))
        {
            return row => ArrowTypeMap.GetValue(array, row, field) ?? DBNull.Value;
        }

        return array switch
        {
            Int32Array a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            Int64Array a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            Int16Array a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            Int8Array a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            UInt8Array a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            UInt16Array a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            UInt32Array a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            UInt64Array a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            BooleanArray a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            FloatArray a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            DoubleArray a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetValue(row)!.Value,
            StringArray a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetString(row),
            BinaryArray a => row => a.IsNull(row) ? DBNull.Value : (object)a.GetBytes(row).ToArray(),
            Decimal128Array a => ArrowTypeMap.IsUndeclaredDecimal(field)
                ? row => a.GetValue(row) is { } d ? (object)DecimalWidth.Bare(d) : DBNull.Value
                : row => a.GetValue(row) is { } d ? (object)d : DBNull.Value,
            Decimal256Array a => ArrowTypeMap.IsUndeclaredDecimal(field)
                ? row => a.GetValue(row) is { } d ? (object)DecimalWidth.Bare(d) : DBNull.Value
                : row => a.GetValue(row) is { } d ? (object)d : DBNull.Value,
            Date32Array a => row => a.IsNull(row) ? DBNull.Value : (a.GetDateOnly(row) is { } d32 ? (object)d32 : DBNull.Value),
            Date64Array a => row => a.IsNull(row) ? DBNull.Value : (a.GetDateTime(row) is { } d64 ? (object)d64 : DBNull.Value),
            TimestampArray a => field.DataType is TimestampType ts && string.IsNullOrEmpty(ts.Timezone)
                ? row => a.GetTimestamp(row) is { } dto ? (object)TemporalNormalization.ToWallClock(dto) : DBNull.Value
                : row => a.GetTimestamp(row) is { } dto ? (object)dto : DBNull.Value,
            _ => row => ArrowTypeMap.GetValue(array, row, field) ?? DBNull.Value
        };
    }

    // ── Core navigation ──────────────────────────────────────────────────────

    public override bool Read()
    {
        _row++;
        return _row < _batch.Length;
    }

    public override bool NextResult() => false;  // single result set

    public override void Close() => _closed = true;

    // ── Schema ───────────────────────────────────────────────────────────────

    public override int FieldCount => _fieldCount;

    public override bool HasRows => _batch.Length > 0;

    public override bool IsClosed => _closed;

    public override int RecordsAffected => -1;

    public override int Depth => 0;

    public override string GetName(int ordinal) => _fields[ordinal].Name;

    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < _fieldCount; i++)
            if (string.Equals(_fields[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public override string GetDataTypeName(int ordinal)
        => GetFieldType(ordinal).Name;

    public override Type GetFieldType(int ordinal)
        => _fieldTypes[ordinal];

    // ── Null check ───────────────────────────────────────────────────────────

    public override bool IsDBNull(int ordinal) => _columns[ordinal].IsNull(_row);

    // ── Value access ─────────────────────────────────────────────────────────

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override object GetValue(int ordinal)
        => _getters[ordinal](_row);

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, _fieldCount);
        for (int i = 0; i < count; i++)
            values[i] = _getters[i](_row);
        return count;
    }

    // ── Typed accessors ───────────────────────────────────────────────────────
    // These allow higher-level callers to avoid boxing by calling GetFieldValue<T> directly.
    // SqlBulkCopy uses GetValue() which boxes; these typed methods are available for callers
    // that support the DbDataReader typed API.

    public override bool GetBoolean(int ordinal)
        => _columns[ordinal] is BooleanArray ba ? ba.GetValue(_row)!.Value : (bool)GetValue(ordinal);
    public override byte GetByte(int ordinal)
        => _columns[ordinal] is UInt8Array ua ? ua.GetValue(_row)!.Value : (byte)GetValue(ordinal);
    public override short GetInt16(int ordinal)
        => _columns[ordinal] is Int16Array ia ? ia.GetValue(_row)!.Value : (short)GetValue(ordinal);
    public override int GetInt32(int ordinal)
        => _columns[ordinal] is Int32Array ia ? ia.GetValue(_row)!.Value : (int)GetValue(ordinal);
    public override long GetInt64(int ordinal)
        => _columns[ordinal] is Int64Array ia ? ia.GetValue(_row)!.Value : (long)GetValue(ordinal);
    public override float GetFloat(int ordinal)
        => _columns[ordinal] is FloatArray fa ? fa.GetValue(_row)!.Value : (float)GetValue(ordinal);
    public override double GetDouble(int ordinal)
        => _columns[ordinal] is DoubleArray da ? da.GetValue(_row)!.Value : (double)GetValue(ordinal);
    public override decimal GetDecimal(int ordinal)
        => (decimal)GetValue(ordinal);
    public override string GetString(int ordinal)
        => _columns[ordinal] is StringArray sa ? sa.GetString(_row) : (string)GetValue(ordinal);

    public override DateTime GetDateTime(int ordinal)
        => _columns[ordinal] switch
        {
            Date32Array d32 => d32.GetDateTime(_row)!.Value,
            Date64Array d64 => d64.GetDateTime(_row)!.Value,
            _ => (DateTime)GetValue(ordinal)
        };
    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

    public override char GetChar(int ordinal) => throw new NotSupportedException();

    // ── Bounded copies ───────────────────────────────────────────────────────
    // A caller that reads a long value in pieces (DbDataReader.GetStream, a bulk loader streaming a
    // column) copies it through these, per the DbDataReader contract: the value's length when the
    // buffer is null, else at most `length` items from `dataOffset`, and 0 from its end on. GetBytes
    // of a value that is not bytes, or GetChars of one that is not text, is an InvalidCastException,
    // as a cast of GetValue would be.

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => _bytes[ordinal] is { } array && !array.IsNull(_row)
            ? CopyOut(array.GetBytes(_row), dataOffset, buffer, bufferOffset, length)
            : CopyOut(GetValue(ordinal) as byte[] ?? throw NotA(ordinal, "bytes"), dataOffset, buffer, bufferOffset, length);

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => CopyOut((GetValue(ordinal) as string ?? throw NotA(ordinal, "text")).AsSpan(), dataOffset, buffer, bufferOffset, length);

    private static long CopyOut<T>(ReadOnlySpan<T> value, long dataOffset, T[]? buffer, int bufferOffset, int length)
    {
        if (buffer is null) return value.Length;
        ArgumentOutOfRangeException.ThrowIfNegative(dataOffset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if ((uint)bufferOffset > (uint)buffer.Length) throw new ArgumentOutOfRangeException(nameof(bufferOffset));
        if (length > buffer.Length - bufferOffset) throw new ArgumentOutOfRangeException(nameof(length));
        if (dataOffset >= value.Length) return 0;

        var count = (int)Math.Min(length, value.Length - dataOffset);
        value.Slice((int)dataOffset, count).CopyTo(buffer.AsSpan(bufferOffset));
        return count;
    }

    private InvalidCastException NotA(int ordinal, string kind)
        => new($"Column '{_fields[ordinal].Name}' holds {(IsDBNull(ordinal) ? "a null" : _fieldTypes[ordinal].Name)} at this row, not {kind}.");

    // ── Schema table (not needed for SqlBulkCopy) ────────────────────────────

    public override DataTable? GetSchemaTable() => null;

    // ── Enumerator ───────────────────────────────────────────────────────────

    public override IEnumerator GetEnumerator()
        => new DbEnumerator(this, closeReader: false);

}
