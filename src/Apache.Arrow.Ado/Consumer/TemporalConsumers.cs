using System;
using System.IO;
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
            // Not new DateTimeOffset(dt): it resolves a zone-less value against the local zone.
            Builder.Append(Apache.Arrow.Serialization.Mapping.TemporalNormalization.ToOffset(dt));
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

/// <summary>
/// A time of day as microseconds since midnight. The driver's <see cref="TimeSpan"/> is a clock reading only
/// inside [0, 24h): a value outside it (MySQL <c>TIME</c> reaches 838 hours and may be negative) is an elapsed
/// time, not a time of day, and is refused by column name rather than written as a wrong hour.
/// </summary>
public sealed class Time64Consumer : BaseAdoConsumer<Time64Array.Builder, Time64Array, Time64Type>
{
    public Time64Consumer(int columnIndex) : base(columnIndex, new Time64Type(TimeUnit.Microsecond), new Time64Array.Builder(new Time64Type(TimeUnit.Microsecond))) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        var obj = reader.GetValue(ColumnIndex);
        if (obj is TimeSpan ts)
            Builder.Append(ToMicroseconds(ts, reader.GetName(ColumnIndex)));
        else
            Builder.AppendNull();
    }

    /// <summary>Microseconds since midnight; a value outside one day is refused naming <paramref name="columnName"/>.</summary>
    public static long ToMicroseconds(TimeSpan value, string columnName)
    {
        if (value < TimeSpan.Zero || value >= TimeSpan.FromDays(1))
            throw new InvalidDataException(
                $"Column '{columnName}' holds {value}, which is not a time of day (0 to 24 hours). " +
                "Cast the column to text in the query to read it.");
        return value.Ticks / TimeSpan.TicksPerMicrosecond;
    }
}

public sealed class Decimal128Consumer : BaseAdoConsumer<Decimal128Array.Builder, Decimal128Array, Decimal128Type>
{
    private readonly int _scale;
    public Decimal128Consumer(int columnIndex, Decimal128Type type) : base(columnIndex, type, new Decimal128Array.Builder(type)) => _scale = type.Scale;
    protected override void ConsumeValue(DbDataReader reader)
    {
        Builder.Append(Apache.Arrow.Serialization.Mapping.DecimalWidth.ForBuilder(reader.GetDecimal(ColumnIndex), _scale));
    }
}

public sealed class Decimal256Consumer : BaseAdoConsumer<Decimal256Array.Builder, Decimal256Array, Decimal256Type>
{
    private readonly int _scale;
    public Decimal256Consumer(int columnIndex, Decimal256Type type) : base(columnIndex, type, new Decimal256Array.Builder(type)) => _scale = type.Scale;
    protected override void ConsumeValue(DbDataReader reader)
    {
        Builder.Append(Apache.Arrow.Serialization.Mapping.DecimalWidth.ForBuilder(reader.GetDecimal(ColumnIndex), _scale));
    }
}
