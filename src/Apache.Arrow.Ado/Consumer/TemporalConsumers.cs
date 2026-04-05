using System;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado.Consumer;

public sealed class Date32Consumer : BaseAdoConsumer<Date32Array.Builder, Date32Array, Date32Type>
{
    public Date32Consumer(int columnIndex) : base(columnIndex, Date32Type.Default, new Date32Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        Builder.Append(reader.GetDateTime(ColumnIndex));
    }
}

public sealed class Date64Consumer : BaseAdoConsumer<Date64Array.Builder, Date64Array, Date64Type>
{
    public Date64Consumer(int columnIndex) : base(columnIndex, Date64Type.Default, new Date64Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        Builder.Append(reader.GetDateTime(ColumnIndex));
    }
}

public sealed class TimestampConsumer : BaseAdoConsumer<TimestampArray.Builder, TimestampArray, TimestampType>
{
    public TimestampConsumer(int columnIndex, TimestampType type)
        : base(columnIndex, type, new TimestampArray.Builder(type)) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        var obj = reader.GetValue(ColumnIndex);
        if (obj is DateTimeOffset dto)
            Builder.Append(dto);
        else if (obj is DateTime dt)
            Builder.Append(new DateTimeOffset(dt));
        else
            Builder.AppendNull();
    }
}

public sealed class DurationConsumer : BaseAdoConsumer<DurationArray.Builder, DurationArray, DurationType>
{
    public DurationConsumer(int columnIndex) : base(columnIndex, DurationType.Millisecond, new DurationArray.Builder(DurationType.Millisecond)) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        var obj = reader.GetValue(ColumnIndex);
        if (obj is TimeSpan ts)
            Builder.Append(ts.Ticks / TimeSpan.TicksPerMillisecond);
        else
            Builder.AppendNull();
    }
}

public sealed class Time32Consumer : BaseAdoConsumer<Time32Array.Builder, Time32Array, Time32Type>
{
    public Time32Consumer(int columnIndex) : base(columnIndex, new Time32Type(TimeUnit.Millisecond), new Time32Array.Builder(new Time32Type(TimeUnit.Millisecond))) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        var obj = reader.GetValue(ColumnIndex);
        if (obj is TimeSpan ts)
            Builder.Append((int)(ts.Ticks / TimeSpan.TicksPerMillisecond));
        else
            Builder.AppendNull();
    }
}

public sealed class Decimal128Consumer : BaseAdoConsumer<Decimal128Array.Builder, Decimal128Array, Decimal128Type>
{
    public Decimal128Consumer(int columnIndex, Decimal128Type type) : base(columnIndex, type, new Decimal128Array.Builder(type)) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        Builder.Append(reader.GetDecimal(ColumnIndex));
    }
}

public sealed class Decimal256Consumer : BaseAdoConsumer<Decimal256Array.Builder, Decimal256Array, Decimal256Type>
{
    public Decimal256Consumer(int columnIndex, Decimal256Type type) : base(columnIndex, type, new Decimal256Array.Builder(type)) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        Builder.Append(reader.GetDecimal(ColumnIndex));
    }
}
