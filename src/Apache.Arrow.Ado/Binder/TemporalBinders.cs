using System;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado.Binder;

/// <summary>Date32 (days since epoch) Arrow → DbParameter binder.</summary>
public sealed class Date32Binder : BaseArrowBinder<Date32Array>
{
    public Date32Binder() : base(Date32Type.Default, (int)System.Data.DbType.Date) { }
    protected override void BindValue(Date32Array a, int row, DbParameter p)
        => p.Value = a.GetDateTime(row);
}

/// <summary>Date64 (ms since epoch) Arrow → DbParameter binder.</summary>
public sealed class Date64Binder : BaseArrowBinder<Date64Array>
{
    public Date64Binder() : base(Date64Type.Default, (int)System.Data.DbType.DateTime) { }
    protected override void BindValue(Date64Array a, int row, DbParameter p)
        => p.Value = a.GetDateTime(row);
}

/// <summary>Timestamp Arrow → DbParameter binder. Returns <see cref="DateTimeOffset"/>.</summary>
public sealed class TimestampBinder : BaseArrowBinder<TimestampArray>
{
    public TimestampBinder() : base(TimestampType.Default, (int)System.Data.DbType.DateTimeOffset) { }
    protected override void BindValue(TimestampArray a, int row, DbParameter p)
    {
        var ts = a.GetTimestamp(row);
        p.Value = ts.HasValue ? (object)ts.Value : DBNull.Value;
    }
}

/// <summary>Duration Arrow → DbParameter binder. Returns <see cref="TimeSpan"/> in milliseconds.</summary>
public sealed class DurationBinder : BaseArrowBinder<DurationArray>
{
    public DurationBinder() : base(DurationType.Millisecond, (int)System.Data.DbType.Int64) { }
    protected override void BindValue(DurationArray a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>Decimal128 Arrow → DbParameter binder.</summary>
public sealed class Decimal128Binder : BaseArrowBinder<Decimal128Array>
{
    public Decimal128Binder(Decimal128Type type) : base(type, (int)System.Data.DbType.Decimal) { }
    protected override void BindValue(Decimal128Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row);
}

/// <summary>Decimal256 Arrow → DbParameter binder (down-cast to decimal).</summary>
public sealed class Decimal256Binder : BaseArrowBinder<Decimal256Array>
{
    public Decimal256Binder(Decimal256Type type) : base(type, (int)System.Data.DbType.Decimal) { }
    protected override void BindValue(Decimal256Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row);
}
