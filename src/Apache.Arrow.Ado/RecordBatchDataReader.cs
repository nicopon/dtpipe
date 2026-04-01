using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
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
    private int _row = -1;
    private bool _closed;

    /// <summary>Creates a reader over the given batch. The batch is NOT disposed by this reader.</summary>
    public RecordBatchDataReader(RecordBatch batch)
    {
        _batch = batch ?? throw new ArgumentNullException(nameof(batch));
        _schema = batch.Schema;
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

    public override int FieldCount => _schema.FieldsList.Count;

    public override bool HasRows => _batch.Length > 0;

    public override bool IsClosed => _closed;

    public override int RecordsAffected => -1;

    public override int Depth => 0;

    public override string GetName(int ordinal) => _schema.FieldsList[ordinal].Name;

    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < _schema.FieldsList.Count; i++)
            if (string.Equals(_schema.FieldsList[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public override string GetDataTypeName(int ordinal)
        => GetFieldType(ordinal).Name;

    public override Type GetFieldType(int ordinal)
        => GetClrType(_schema.FieldsList[ordinal].DataType);

    // ── Null check ───────────────────────────────────────────────────────────

    public override bool IsDBNull(int ordinal) => _batch.Column(ordinal).IsNull(_row);

    // ── Value access ─────────────────────────────────────────────────────────

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name]  => GetValue(GetOrdinal(name));

    public override object GetValue(int ordinal)
        => ExtractValue(_batch.Column(ordinal), _row) ?? DBNull.Value;

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);
        return count;
    }

    // ── Typed accessors ───────────────────────────────────────────────────────
    // These allow higher-level callers to avoid boxing by calling GetFieldValue<T> directly.
    // SqlBulkCopy uses GetValue() which boxes; these typed methods are available for callers
    // that support the DbDataReader typed API.

    public override bool    GetBoolean(int ordinal) => ((BooleanArray)_batch.Column(ordinal)).GetValue(_row)!.Value;
    public override byte    GetByte   (int ordinal) => ((UInt8Array)  _batch.Column(ordinal)).GetValue(_row)!.Value;
    public override short   GetInt16  (int ordinal) => ((Int16Array)  _batch.Column(ordinal)).GetValue(_row)!.Value;
    public override int     GetInt32  (int ordinal) => ((Int32Array)  _batch.Column(ordinal)).GetValue(_row)!.Value;
    public override long    GetInt64  (int ordinal) => ((Int64Array)  _batch.Column(ordinal)).GetValue(_row)!.Value;
    public override float   GetFloat  (int ordinal) => ((FloatArray)  _batch.Column(ordinal)).GetValue(_row)!.Value;
    public override double  GetDouble (int ordinal) => ((DoubleArray) _batch.Column(ordinal)).GetValue(_row)!.Value;
    public override decimal GetDecimal(int ordinal) => ((Decimal128Array)_batch.Column(ordinal)).GetValue(_row)!.Value;
    public override string  GetString (int ordinal) => ((StringArray) _batch.Column(ordinal)).GetString(_row)!;

    public override DateTime GetDateTime(int ordinal)
    {
        var arr = _batch.Column(ordinal);
        return arr switch
        {
            Date32Array d32 => d32.GetDateTime(_row) ?? throw new InvalidCastException(),
            Date64Array d64 => d64.GetDateTime(_row) ?? throw new InvalidCastException(),
            TimestampArray ts => ts.GetTimestamp(_row)?.DateTime ?? throw new InvalidCastException(),
            _ => throw new InvalidCastException($"Cannot convert {arr.GetType().Name} to DateTime.")
        };
    }

    public override Guid GetGuid(int ordinal)
    {
        // DtPipe convention: UUID stored as RFC 4122 big-endian bytes in either
        // BinaryArray (legacy BinaryType) or FixedSizeBinaryArray (FixedSizeBinaryType(16) + arrow.uuid).
        var arr = _batch.Column(ordinal);
        ReadOnlySpan<byte> bytes = arr switch
        {
            BinaryArray b          => b.GetBytes(_row),
            FixedSizeBinaryArray f => f.GetBytes(_row),
            _ => throw new InvalidCastException(
                     $"Cannot read column of type {arr.GetType().Name} as Guid.")
        };
        if (bytes.Length != 16)
            throw new InvalidCastException("Binary column is not a 16-byte UUID.");
        // RFC 4122 big-endian → .NET Guid (little-endian first 3 components)
        var copy = bytes.ToArray();
        System.Array.Reverse(copy, 0, 4);
        System.Array.Reverse(copy, 4, 2);
        System.Array.Reverse(copy, 6, 2);
        return new Guid(copy);
    }

    // These are rarely needed by SqlBulkCopy but required by the abstract contract:
    public override char GetChar(int ordinal) => throw new NotSupportedException();
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException("Use GetValue() to retrieve binary data as byte[].");
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => throw new NotSupportedException();

    // ── Schema table (not needed for SqlBulkCopy) ────────────────────────────

    public override DataTable? GetSchemaTable() => null;

    // ── Enumerator ───────────────────────────────────────────────────────────

    public override IEnumerator GetEnumerator()
        => new DbEnumerator(this, closeReader: false);

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Extracts the value at the given row from an Arrow array, returning a boxed CLR value
    /// or <see langword="null"/> for Arrow nulls.
    /// </summary>
    private static object? ExtractValue(IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;
        return array switch
        {
            BooleanArray       a => a.GetValue(index),
            Int8Array          a => a.GetValue(index),
            Int16Array         a => a.GetValue(index),
            Int32Array         a => a.GetValue(index),
            Int64Array         a => a.GetValue(index),
            UInt8Array         a => a.GetValue(index),
            UInt16Array        a => a.GetValue(index),
            UInt32Array        a => a.GetValue(index),
            UInt64Array        a => a.GetValue(index),
            FloatArray         a => a.GetValue(index),
            DoubleArray        a => a.GetValue(index),
            StringArray        a => a.GetString(index),
            BinaryArray        a => a.GetBytes(index).ToArray(),
            // Decimal arrays BEFORE FixedSizeBinaryArray (Decimal128/256 inherit from FixedSizeBinaryArray)
            Decimal128Array    a => a.GetValue(index),
            Decimal256Array    a => a.GetValue(index),
            // FixedSizeBinary: return raw byte[] — no semantic inference (e.g. no Guid heuristic).
            // Callers with Field context should use ArrowTypeMapper.GetValueForField to resolve
            // extension types such as arrow.uuid.
            FixedSizeBinaryArray a => a.GetBytes(index).ToArray(),
            Date32Array        a => (object?)a.GetDateTime(index),
            Date64Array        a => (object?)a.GetDateTime(index),
            TimestampArray     a => (object?)a.GetTimestamp(index),
            DurationArray      a => a.GetValue(index),
            Time32Array        a => a.GetValue(index),
            Time64Array        a => a.GetValue(index),
            _ => null
        };
    }

    /// <summary>Maps an Arrow type to the closest .NET CLR type.</summary>
    private static Type GetClrType(IArrowType type) => type.TypeId switch
    {
        ArrowTypeId.Boolean   => typeof(bool),
        ArrowTypeId.Int8      => typeof(sbyte),
        ArrowTypeId.UInt8     => typeof(byte),
        ArrowTypeId.Int16     => typeof(short),
        ArrowTypeId.UInt16    => typeof(ushort),
        ArrowTypeId.Int32     => typeof(int),
        ArrowTypeId.UInt32    => typeof(uint),
        ArrowTypeId.Int64     => typeof(long),
        ArrowTypeId.UInt64    => typeof(ulong),
        ArrowTypeId.Float     => typeof(float),
        ArrowTypeId.Double    => typeof(double),
        ArrowTypeId.String    => typeof(string),
        ArrowTypeId.Binary    => typeof(byte[]),
        ArrowTypeId.Date32    => typeof(DateTime),
        ArrowTypeId.Date64    => typeof(DateTime),
        ArrowTypeId.Timestamp => typeof(DateTimeOffset),
        ArrowTypeId.Decimal128 => typeof(decimal),
        ArrowTypeId.Decimal256 => typeof(decimal),
        ArrowTypeId.Duration  => typeof(TimeSpan),
        _ => typeof(object)
    };
}
