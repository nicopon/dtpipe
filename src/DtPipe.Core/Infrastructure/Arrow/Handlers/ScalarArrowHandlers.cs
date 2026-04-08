using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Apache.Arrow.Serialization.Reflection;
using System;

namespace DtPipe.Core.Infrastructure.Arrow.Handlers;

internal abstract class ScalarArrowHandler<TType, TArray, TBuilder> : IArrowTypeHandler
    where TType : IArrowType
    where TArray : IArrowArray
    where TBuilder : IArrowArrayBuilder
{
    public virtual bool CanHandle(IArrowType type) => type is TType;
    public virtual bool CanHandle(IArrowArrayBuilder builder) => builder is TBuilder;

    public abstract IArrowArrayBuilder CreateBuilder(IArrowType type);

    public virtual void AppendNull(IArrowArrayBuilder builder)
    {
        if (builder is TBuilder b)
        {
            // Most Arrow builders have an AppendNull method. 
            // We'll use reflection or a dynamic cast if necessary, but 
            // for now, we'll implement it in subclasses to be safe and typed.
            AppendNullTyped(b);
        }
    }

    protected abstract void AppendNullTyped(TBuilder builder);

    public virtual IArrowArray Build(IArrowArrayBuilder builder)
    {
        return builder is TBuilder b ? BuildTyped(b) : throw new InvalidOperationException();
    }

    protected abstract IArrowArray BuildTyped(TBuilder builder);

    public virtual void AppendValue(IArrowArrayBuilder builder, object? value)
    {
        if (value == null || value == DBNull.Value)
        {
            AppendNull(builder);
            return;
        }
        if (builder is TBuilder b) AppendValueTyped(b, value);
    }

    protected abstract void AppendValueTyped(TBuilder builder, object value);
}

internal class BooleanHandler : ScalarArrowHandler<BooleanType, BooleanArray, BooleanArray.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new BooleanArray.Builder();
    protected override void AppendNullTyped(BooleanArray.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(BooleanArray.Builder b) => b.Build();
    protected override void AppendValueTyped(BooleanArray.Builder b, object v) => b.Append(Convert.ToBoolean(v));
}

internal class Int8Handler : ScalarArrowHandler<Int8Type, Int8Array, Int8Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Int8Array.Builder();
    protected override void AppendNullTyped(Int8Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Int8Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Int8Array.Builder b, object v) => b.Append(Convert.ToSByte(v));
}

internal class Int16Handler : ScalarArrowHandler<Int16Type, Int16Array, Int16Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Int16Array.Builder();
    protected override void AppendNullTyped(Int16Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Int16Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Int16Array.Builder b, object v) => b.Append(Convert.ToInt16(v));
}

internal class Int32Handler : ScalarArrowHandler<Int32Type, Int32Array, Int32Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Int32Array.Builder();
    protected override void AppendNullTyped(Int32Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Int32Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Int32Array.Builder b, object v) => b.Append(Convert.ToInt32(v));
}

internal class Int64Handler : ScalarArrowHandler<Int64Type, Int64Array, Int64Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Int64Array.Builder();
    protected override void AppendNullTyped(Int64Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Int64Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Int64Array.Builder b, object v) => b.Append(Convert.ToInt64(v));
}

internal class UInt8Handler : ScalarArrowHandler<UInt8Type, UInt8Array, UInt8Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new UInt8Array.Builder();
    protected override void AppendNullTyped(UInt8Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(UInt8Array.Builder b) => b.Build();
    protected override void AppendValueTyped(UInt8Array.Builder b, object v) => b.Append(Convert.ToByte(v));
}

internal class UInt16Handler : ScalarArrowHandler<UInt16Type, UInt16Array, UInt16Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new UInt16Array.Builder();
    protected override void AppendNullTyped(UInt16Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(UInt16Array.Builder b) => b.Build();
    protected override void AppendValueTyped(UInt16Array.Builder b, object v) => b.Append(Convert.ToUInt16(v));
}

internal class UInt32Handler : ScalarArrowHandler<UInt32Type, UInt32Array, UInt32Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new UInt32Array.Builder();
    protected override void AppendNullTyped(UInt32Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(UInt32Array.Builder b) => b.Build();
    protected override void AppendValueTyped(UInt32Array.Builder b, object v) => b.Append(Convert.ToUInt32(v));
}

internal class UInt64Handler : ScalarArrowHandler<UInt64Type, UInt64Array, UInt64Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new UInt64Array.Builder();
    protected override void AppendNullTyped(UInt64Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(UInt64Array.Builder b) => b.Build();
    protected override void AppendValueTyped(UInt64Array.Builder b, object v) => b.Append(Convert.ToUInt64(v));
}

internal class FloatHandler : ScalarArrowHandler<FloatType, FloatArray, FloatArray.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new FloatArray.Builder();
    protected override void AppendNullTyped(FloatArray.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(FloatArray.Builder b) => b.Build();
    protected override void AppendValueTyped(FloatArray.Builder b, object v) => b.Append(Convert.ToSingle(v));
}

internal class DoubleHandler : ScalarArrowHandler<DoubleType, DoubleArray, DoubleArray.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new DoubleArray.Builder();
    protected override void AppendNullTyped(DoubleArray.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(DoubleArray.Builder b) => b.Build();
    protected override void AppendValueTyped(DoubleArray.Builder b, object v) => b.Append(Convert.ToDouble(v));
}

internal class StringHandler : ScalarArrowHandler<StringType, StringArray, StringArray.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new StringArray.Builder();
    protected override void AppendNullTyped(StringArray.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(StringArray.Builder b) => b.Build();
    protected override void AppendValueTyped(StringArray.Builder b, object v) => b.Append(v.ToString());
}

internal class BinaryHandler : ScalarArrowHandler<BinaryType, BinaryArray, BinaryArray.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new BinaryArray.Builder();
    protected override void AppendNullTyped(BinaryArray.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(BinaryArray.Builder b) => b.Build();
    protected override void AppendValueTyped(BinaryArray.Builder b, object v)
    {
        if (v is byte[] bytes) b.Append((System.Collections.Generic.IEnumerable<byte>)bytes);
        else b.AppendNull();
    }
}

internal class Decimal128Handler : ScalarArrowHandler<Decimal128Type, Decimal128Array, Decimal128Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Decimal128Array.Builder((Decimal128Type)type);
    protected override void AppendNullTyped(Decimal128Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Decimal128Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Decimal128Array.Builder b, object v) => b.Append(Convert.ToDecimal(v));
}

internal class Decimal256Handler : ScalarArrowHandler<Decimal256Type, Decimal256Array, Decimal256Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Decimal256Array.Builder((Decimal256Type)type);
    protected override void AppendNullTyped(Decimal256Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Decimal256Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Decimal256Array.Builder b, object v) => b.Append(Convert.ToDecimal(v));
}

internal class FixedSizeBinaryHandler : ScalarArrowHandler<FixedSizeBinaryType, FixedSizeBinaryArray, FixedSizeBinaryArrayBuilder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new FixedSizeBinaryArrayBuilder(((FixedSizeBinaryType)type).ByteWidth);
    protected override void AppendNullTyped(FixedSizeBinaryArrayBuilder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(FixedSizeBinaryArrayBuilder b) => b.Build();
    protected override void AppendValueTyped(FixedSizeBinaryArrayBuilder b, object v)
    {
        if (v is Guid guid) b.Append(ArrowTypeMapper.ToArrowUuidBytes(guid));
        else if (v is byte[] bytes && bytes.Length == b.ByteWidth) b.Append(bytes);
        else b.AppendNull();
    }
}

internal class Date32Handler : ScalarArrowHandler<Date32Type, Date32Array, Date32Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Date32Array.Builder();
    protected override void AppendNullTyped(Date32Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Date32Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Date32Array.Builder b, object v)
    {
        if (v is DateOnly d) b.Append(d.ToDateTime(TimeOnly.MinValue));
        else b.Append(Convert.ToDateTime(v));
    }
}

internal class Date64Handler : ScalarArrowHandler<Date64Type, Date64Array, Date64Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Date64Array.Builder();
    protected override void AppendNullTyped(Date64Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Date64Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Date64Array.Builder b, object v)
    {
        if (v is DateOnly d) b.Append(d.ToDateTime(TimeOnly.MinValue));
        else b.Append(Convert.ToDateTime(v));
    }
}

internal class TimestampHandler : ScalarArrowHandler<TimestampType, TimestampArray, TimestampArray.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new TimestampArray.Builder((TimestampType)type);
    protected override void AppendNullTyped(TimestampArray.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(TimestampArray.Builder b) => b.Build();
    protected override void AppendValueTyped(TimestampArray.Builder b, object v)
    {
        if (v is DateTimeOffset dto) b.Append(dto);
        else if (v is DateTime dt) b.Append(dt);
        else if (v is DateOnly d) b.Append(d.ToDateTime(TimeOnly.MinValue));
        else b.Append(Convert.ToDateTime(v));
    }
}

internal class DurationHandler : ScalarArrowHandler<DurationType, DurationArray, DurationArray.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new DurationArray.Builder((DurationType)type);
    protected override void AppendNullTyped(DurationArray.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(DurationArray.Builder b) => b.Build();
    protected override void AppendValueTyped(DurationArray.Builder b, object v) => b.Append((long)Convert.ChangeType(v, typeof(long)));
}

internal class Time32Handler : ScalarArrowHandler<Time32Type, Time32Array, Time32Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Time32Array.Builder((Time32Type)type);
    protected override void AppendNullTyped(Time32Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Time32Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Time32Array.Builder b, object v) => b.Append(Convert.ToInt32(v));
}

internal class Time64Handler : ScalarArrowHandler<Time64Type, Time64Array, Time64Array.Builder>
{
    public override IArrowArrayBuilder CreateBuilder(IArrowType type) => new Time64Array.Builder((Time64Type)type);
    protected override void AppendNullTyped(Time64Array.Builder b) => b.AppendNull();
    protected override IArrowArray BuildTyped(Time64Array.Builder b) => b.Build();
    protected override void AppendValueTyped(Time64Array.Builder b, object v)
    {
        if (v is TimeOnly t) b.Append(t.ToTimeSpan().Ticks * 100);
        else b.Append(Convert.ToInt64(v));
    }
}
