namespace DtPipe.Core.Cursor;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

public class CursorTrackingRowDecorator : IRowDataWriter, ICursorTracker
{
    private readonly IDataWriter _inner;
    private readonly string _cursorColumn;
    private PipeColumnInfo? _cursorColumnInfo;
    private CursorType _cursorType;
    private object? _maxValueRaw;

    protected int CursorColumnIndex { get; private set; } = -1;

    public CursorTrackingRowDecorator(IDataWriter inner, string cursorColumn)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _cursorColumn = cursorColumn ?? throw new ArgumentNullException(nameof(cursorColumn));
    }

    public CursorValue? TrackedMaxValue
    {
        get
        {
            if (_maxValueRaw == null) return null;

            // Every digit the value carries, at least three: a bound shorter than the column makes
            // `col > bound` true for the row that produced it, and a strict `>` re-reads it on every
            // run. A whole-millisecond value keeps its three digits, the literal a SQL Server
            // `datetime` or an Oracle `FF3` mask accepts.
            string formattedValue;
            if (_maxValueRaw is DateTime dt)
            {
                formattedValue = dt.ToString("yyyy-MM-ddTHH:mm:ss.", CultureInfo.InvariantCulture) + Fraction(dt.Ticks);
            }
            else if (_maxValueRaw is DateTimeOffset dto)
            {
                formattedValue = dto.ToString("yyyy-MM-ddTHH:mm:ss.", CultureInfo.InvariantCulture) + Fraction(dto.Ticks)
                    + dto.ToString("zzz", CultureInfo.InvariantCulture);
            }
            else if (_maxValueRaw is DateOnly d)
            {
                // An ISO 8601 date in the invariant culture: the literal a SQL engine compares with a
                // date column whatever its session's date order (Oracle through `DATE '...'`). The
                // process culture's short date is refused by some engines and read day-first by others.
                formattedValue = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }
            else
            {
                formattedValue = _maxValueRaw.ToString() ?? "";
            }

            return new CursorValue(_cursorColumn, formattedValue, _cursorType);
        }
    }

    /// <summary>The seven digits of the second's fraction, without the zeros that follow the third.</summary>
    private static string Fraction(long ticks)
    {
        var digits = (ticks % TimeSpan.TicksPerSecond).ToString("D7", CultureInfo.InvariantCulture);
        var length = digits.Length;
        while (length > 3 && digits[length - 1] == '0') length--;
        return digits[..length];
    }

    public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        var tempIndex = -1;
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, _cursorColumn, StringComparison.OrdinalIgnoreCase))
            {
                tempIndex = i;
                _cursorColumnInfo = columns[i];
                break;
            }
        }

        if (tempIndex == -1)
        {
            throw new InvalidOperationException($"Cursor column '{_cursorColumn}' not found in target schema.");
        }

        CursorColumnIndex = tempIndex;
        _cursorType = DetermineCursorType(_cursorColumnInfo!.ClrType);

        await _inner.InitializeAsync(columns, ct);
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        if (_inner is IRowDataWriter rowWriter)
        {
            foreach (var row in rows)
            {
                if (row.Length > CursorColumnIndex)
                {
                    var val = row[CursorColumnIndex];
                    ProcessTrackedValue(val);
                }
            }
            await rowWriter.WriteBatchAsync(rows, ct);
        }
        else
        {
            throw new InvalidOperationException("Inner writer does not support row-based data writing.");
        }
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        return _inner.CompleteAsync(ct);
    }

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        return _inner.ExecuteCommandAsync(command, ct);
    }

    public ValueTask DisposeAsync()
    {
        return _inner.DisposeAsync();
    }

    protected void ProcessTrackedValue(object? val)
    {
        if (val == null || val == DBNull.Value) return;

        if (_maxValueRaw == null)
        {
            _maxValueRaw = val;
        }
        else
        {
            if (CompareValues(val, _maxValueRaw) > 0)
            {
                _maxValueRaw = val;
            }
        }
    }

    private static CursorType DetermineCursorType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        if (underlyingType == typeof(DateTime) ||
            underlyingType == typeof(DateTimeOffset) ||
            underlyingType == typeof(DateOnly))
        {
            return CursorType.DateTime;
        }

        if (underlyingType == typeof(int) ||
            underlyingType == typeof(long) ||
            underlyingType == typeof(short) ||
            underlyingType == typeof(byte) ||
            underlyingType == typeof(sbyte) ||
            underlyingType == typeof(uint) ||
            underlyingType == typeof(ulong) ||
            underlyingType == typeof(ushort))
        {
            return CursorType.Integer;
        }

        return CursorType.String;
    }

    private static int CompareValues(object val1, object val2)
    {
        if (val1.GetType() == val2.GetType() && val1 is IComparable comp)
        {
            return comp.CompareTo(val2);
        }

        if (val1 is DateTimeOffset dto1 && val2 is DateTimeOffset dto2)
        {
            return dto1.CompareTo(dto2);
        }
        if (val1 is DateTime dt1 && val2 is DateTime dt2)
        {
            return dt1.CompareTo(dt2);
        }

        try
        {
            var type1 = val1.GetType();
            var val2Converted = Convert.ChangeType(val2, type1);
            if (val1 is IComparable comp1)
            {
                return comp1.CompareTo(val2Converted);
            }
        }
        catch
        {
            // Ignore conversion failure
        }

        var str1 = Convert.ToString(val1) ?? "";
        var str2 = Convert.ToString(val2) ?? "";
        return string.CompareOrdinal(str1, str2);
    }
}

public class CursorTrackingColumnarDecorator : CursorTrackingRowDecorator, IColumnarDataWriter
{
    private readonly IColumnarDataWriter _columnarInner;

    public CursorTrackingColumnarDecorator(IColumnarDataWriter inner, string cursorColumn)
        : base(inner, cursorColumn)
    {
        _columnarInner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public async ValueTask WriteRecordBatchAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (CursorColumnIndex >= 0 && CursorColumnIndex < batch.Schema.FieldsList.Count)
        {
            var cursorArray = batch.Column(CursorColumnIndex);
            var cursorField = batch.Schema.FieldsList[CursorColumnIndex];
            for (int i = 0; i < batch.Length; i++)
            {
                if (!cursorArray.IsNull(i))
                {
                    var val = DtPipe.Core.Infrastructure.Arrow.ArrowTypeMapper.GetValueForField(
                        cursorArray, cursorField, i);
                    ProcessTrackedValue(val);
                }
            }
        }
        await _columnarInner.WriteRecordBatchAsync(batch, ct);
    }
}
