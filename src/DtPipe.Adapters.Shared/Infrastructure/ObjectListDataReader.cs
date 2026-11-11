using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DtPipe.Adapters.Shared.Infrastructure;

/// <summary>
/// A <see cref="DbDataReader"/> over a list of rows, which <c>SqlBulkCopy</c> reads without a
/// <see cref="DataTable"/> in between. <see cref="Read"/> maps each written column to its source
/// cell and converts it through the column's converter into one array the reader reuses; a
/// converter returns <see cref="object"/>, so a value type is boxed once per cell.
/// </summary>
public sealed class ObjectListDataReader : DbDataReader
{
    private readonly IReadOnlyList<object?[]> _rows;
    private readonly string[] _columnNames;
    private readonly Type[] _columnTypes;
    private readonly int[] _sourceIndices;
    private readonly Func<object?, object?>[] _converters;
    private readonly object[] _currentRow;
    private readonly int _fieldCount;
    private int _row = -1;
    private bool _closed;

    public ObjectListDataReader(
        IReadOnlyList<object?[]> rows,
        string[] columnNames,
        Type[] columnTypes,
        int[] sourceIndices,
        Func<object?, object?>[] converters)
    {
        _rows = rows ?? throw new ArgumentNullException(nameof(rows));
        _columnNames = columnNames ?? throw new ArgumentNullException(nameof(columnNames));
        _columnTypes = columnTypes ?? throw new ArgumentNullException(nameof(columnTypes));
        _sourceIndices = sourceIndices ?? throw new ArgumentNullException(nameof(sourceIndices));
        _converters = converters ?? throw new ArgumentNullException(nameof(converters));
        _fieldCount = _columnNames.Length;
        _currentRow = new object[_fieldCount];
    }

    public override bool Read()
    {
        _row++;
        if (_row >= _rows.Count)
            return false;

        var srcRow = _rows[_row];
        for (int i = 0; i < _fieldCount; i++)
        {
            int srcIdx = _sourceIndices[i];
            if (srcIdx < 0)
            {
                _currentRow[i] = DBNull.Value;
                continue;
            }
            var raw = srcRow[srcIdx];
            _currentRow[i] = raw is null || raw == DBNull.Value
                ? DBNull.Value
                : _converters[i](raw) ?? DBNull.Value;
        }
        return true;
    }

    public override int FieldCount => _fieldCount;
    public override bool HasRows => _rows.Count > 0;
    public override bool IsClosed => _closed;
    public override void Close() => _closed = true;
    public override int RecordsAffected => -1;
    public override int Depth => 0;
    public override bool NextResult() => false;

    public override string GetName(int ordinal) => _columnNames[ordinal];

    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < _fieldCount; i++)
            if (string.Equals(_columnNames[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public override string GetDataTypeName(int ordinal) => _columnTypes[ordinal].Name;
    public override Type GetFieldType(int ordinal) => _columnTypes[ordinal];

    public override bool IsDBNull(int ordinal)
        => ReferenceEquals(_currentRow[ordinal], DBNull.Value);

    public override object GetValue(int ordinal)
        => _currentRow[ordinal];

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, _fieldCount);
        Array.Copy(_currentRow, 0, values, 0, count);
        return count;
    }

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override bool GetBoolean(int ordinal) => (bool)GetValue(ordinal);
    public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);
    public override short GetInt16(int ordinal) => (short)GetValue(ordinal);
    public override int GetInt32(int ordinal) => (int)GetValue(ordinal);
    public override long GetInt64(int ordinal) => (long)GetValue(ordinal);
    public override float GetFloat(int ordinal) => (float)GetValue(ordinal);
    public override double GetDouble(int ordinal) => (double)GetValue(ordinal);
    public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);
    public override string GetString(int ordinal) => (string)GetValue(ordinal);
    public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);
    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

    public override char GetChar(int ordinal) => throw new NotSupportedException();

    // A caller that reads a long value in pieces (DbDataReader.GetStream, a bulk loader streaming a
    // column) copies it through these, per the DbDataReader contract: the value's length when the
    // buffer is null, else at most `length` items from `dataOffset`, and 0 from its end on. GetBytes
    // of a value that is not bytes, or GetChars of one that is not text, is an InvalidCastException,
    // as a cast of GetValue would be.
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        => CopyOut(_currentRow[ordinal] as byte[] ?? throw NotA(ordinal, "bytes"), dataOffset, buffer, bufferOffset, length);

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => CopyOut((_currentRow[ordinal] as string ?? throw NotA(ordinal, "text")).AsSpan(), dataOffset, buffer, bufferOffset, length);

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
        => new($"Column '{_columnNames[ordinal]}' holds {(IsDBNull(ordinal) ? "a null" : _currentRow[ordinal].GetType().Name)} at this row, not {kind}.");

    public override DataTable? GetSchemaTable() => null;
    public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);
}

