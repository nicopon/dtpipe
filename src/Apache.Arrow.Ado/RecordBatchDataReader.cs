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
        => ArrowTypeMap.GetClrTypeFromField(_schema.FieldsList[ordinal]);

    // ── Null check ───────────────────────────────────────────────────────────

    public override bool IsDBNull(int ordinal) => _batch.Column(ordinal).IsNull(_row);

    // ── Value access ─────────────────────────────────────────────────────────

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name]  => GetValue(GetOrdinal(name));

    public override object GetValue(int ordinal)
        => ArrowTypeMap.GetValue(_batch.Column(ordinal), _row, _schema.FieldsList[ordinal]) ?? DBNull.Value;

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

    public override bool    GetBoolean(int ordinal) => (bool)GetValue(ordinal);
    public override byte    GetByte   (int ordinal) => (byte)GetValue(ordinal);
    public override short   GetInt16  (int ordinal) => (short)GetValue(ordinal);
    public override int     GetInt32  (int ordinal) => (int)GetValue(ordinal);
    public override long    GetInt64  (int ordinal) => (long)GetValue(ordinal);
    public override float   GetFloat  (int ordinal) => (float)GetValue(ordinal);
    public override double  GetDouble (int ordinal) => (double)GetValue(ordinal);
    public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);
    public override string  GetString (int ordinal) => (string)GetValue(ordinal);

    public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);
    public override Guid     GetGuid    (int ordinal) => (Guid)GetValue(ordinal);

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

}
