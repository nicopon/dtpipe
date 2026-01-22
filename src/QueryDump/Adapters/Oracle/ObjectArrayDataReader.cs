using QueryDump.Core;
using System.Data;

namespace QueryDump.Adapters.Oracle;

/// <summary>
/// A lightweight IDataReader implementation that wraps object?[] arrays
/// for efficient use with OracleBulkCopy.WriteToServer(IDataReader).
/// Avoids the overhead of DataTable instantiation for each batch.
/// </summary>
internal sealed class ObjectArrayDataReader : IDataReader
{
    private readonly IReadOnlyList<ColumnInfo> _columns;
    private readonly IReadOnlyList<object?[]> _rows;
    private int _currentIndex = -1;
    private bool _isClosed;

    public ObjectArrayDataReader(IReadOnlyList<ColumnInfo> columns, IReadOnlyList<object?[]> rows)
    {
        _columns = columns ?? throw new ArgumentNullException(nameof(columns));
        _rows = rows ?? throw new ArgumentNullException(nameof(rows));
    }

    public int FieldCount => _columns.Count;

    public int Depth => 0;

    public bool IsClosed => _isClosed;

    public int RecordsAffected => -1;

    public object this[int i] => GetValue(i);

    public object this[string name] => GetValue(GetOrdinal(name));

    public bool Read()
    {
        if (_isClosed) return false;
        _currentIndex++;
        return _currentIndex < _rows.Count;
    }

    public object GetValue(int i)
    {
        if (_currentIndex < 0 || _currentIndex >= _rows.Count)
            throw new InvalidOperationException("No current row.");
        
        return _rows[_currentIndex][i] ?? DBNull.Value;
    }

    public int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, _columns.Count);
        for (int i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }
        return count;
    }

    public bool IsDBNull(int i)
    {
        return _rows[_currentIndex][i] is null || _rows[_currentIndex][i] == DBNull.Value;
    }

    public string GetName(int i) => _columns[i].Name;

    public int GetOrdinal(string name)
    {
        for (int i = 0; i < _columns.Count; i++)
        {
            if (string.Equals(_columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                return i;
        }
        throw new IndexOutOfRangeException($"Column '{name}' not found.");
    }

    public Type GetFieldType(int i) => Nullable.GetUnderlyingType(_columns[i].ClrType) ?? _columns[i].ClrType;

    public string GetDataTypeName(int i) => GetFieldType(i).Name;

    public bool GetBoolean(int i) => (bool)GetValue(i);
    public byte GetByte(int i) => (byte)GetValue(i);
    public char GetChar(int i) => (char)GetValue(i);
    public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
    public decimal GetDecimal(int i) => (decimal)GetValue(i);
    public double GetDouble(int i) => (double)GetValue(i);
    public float GetFloat(int i) => (float)GetValue(i);
    public Guid GetGuid(int i) => (Guid)GetValue(i);
    public short GetInt16(int i) => (short)GetValue(i);
    public int GetInt32(int i) => (int)GetValue(i);
    public long GetInt64(int i) => (long)GetValue(i);
    public string GetString(int i) => (string)GetValue(i);

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var data = (byte[])GetValue(i);
        if (buffer == null) return data.Length;
        
        var count = Math.Min(length, data.Length - (int)fieldOffset);
        Array.Copy(data, (int)fieldOffset, buffer, bufferOffset, count);
        return count;
    }

    public long GetChars(int i, long fieldOffset, char[]? buffer, int bufferOffset, int length)
    {
        var data = GetString(i);
        if (buffer == null) return data.Length;
        
        var count = Math.Min(length, data.Length - (int)fieldOffset);
        data.CopyTo((int)fieldOffset, buffer, bufferOffset, count);
        return count;
    }

    public IDataReader GetData(int i) => throw new NotSupportedException();

    public DataTable GetSchemaTable()
    {
        var schemaTable = new DataTable("SchemaTable");
        schemaTable.Columns.Add("ColumnName", typeof(string));
        schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
        schemaTable.Columns.Add("DataType", typeof(Type));
        schemaTable.Columns.Add("AllowDBNull", typeof(bool));

        for (int i = 0; i < _columns.Count; i++)
        {
            var row = schemaTable.NewRow();
            row["ColumnName"] = _columns[i].Name;
            row["ColumnOrdinal"] = i;
            row["DataType"] = GetFieldType(i);
            row["AllowDBNull"] = true;
            schemaTable.Rows.Add(row);
        }

        return schemaTable;
    }

    public bool NextResult() => false;

    public void Close()
    {
        _isClosed = true;
    }

    public void Dispose()
    {
        Close();
    }
}
