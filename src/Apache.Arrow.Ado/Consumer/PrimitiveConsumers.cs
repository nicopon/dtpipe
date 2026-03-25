using System;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado.Consumer;

/// <summary>
/// Base class for all ADO.NET to Arrow consumers.
/// </summary>
public abstract class BaseAdoConsumer<TBuilder, TArrowArray, TArrowType> : IAdoConsumer
    where TBuilder : IArrowArrayBuilder<TArrowArray, TBuilder>
    where TArrowArray : IArrowArray
    where TArrowType : IArrowType
{
    protected readonly int ColumnIndex;
    protected readonly TArrowType ArrowTypeInstance;
    protected TBuilder Builder;

    protected BaseAdoConsumer(int columnIndex, TArrowType arrowType, TBuilder builder)
    {
        ColumnIndex = columnIndex;
        ArrowTypeInstance = arrowType;
        Builder = builder;
    }

    public IArrowType ArrowType => ArrowTypeInstance;

    public void Consume(DbDataReader reader)
    {
        if (reader.IsDBNull(ColumnIndex))
        {
            Builder.AppendNull();
        }
        else
        {
            ConsumeValue(reader);
        }
    }

    protected abstract void ConsumeValue(DbDataReader reader);

    public IArrowArray BuildArray()
    {
        return Builder.Build(default);
    }

    public void Reset()
    {
        Builder.Clear();
    }

    public virtual void Dispose()
    {
        // Builders don't strictly need disposal, but keeping contract open
    }
}

public sealed class Int8Consumer : BaseAdoConsumer<Int8Array.Builder, Int8Array, Int8Type>
{
    public Int8Consumer(int columnIndex) : base(columnIndex, Int8Type.Default, new Int8Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append((sbyte)reader.GetByte(ColumnIndex));
}

public sealed class Int16Consumer : BaseAdoConsumer<Int16Array.Builder, Int16Array, Int16Type>
{
    public Int16Consumer(int columnIndex) : base(columnIndex, Int16Type.Default, new Int16Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append(reader.GetInt16(ColumnIndex));
}

public sealed class Int32Consumer : BaseAdoConsumer<Int32Array.Builder, Int32Array, Int32Type>
{
    public Int32Consumer(int columnIndex) : base(columnIndex, Int32Type.Default, new Int32Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append(reader.GetInt32(ColumnIndex));
}

public sealed class Int64Consumer : BaseAdoConsumer<Int64Array.Builder, Int64Array, Int64Type>
{
    public Int64Consumer(int columnIndex) : base(columnIndex, Int64Type.Default, new Int64Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append(reader.GetInt64(ColumnIndex));
}

public sealed class UInt8Consumer : BaseAdoConsumer<UInt8Array.Builder, UInt8Array, UInt8Type>
{
    public UInt8Consumer(int columnIndex) : base(columnIndex, UInt8Type.Default, new UInt8Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append(reader.GetByte(ColumnIndex));
}

public sealed class UInt16Consumer : BaseAdoConsumer<UInt16Array.Builder, UInt16Array, UInt16Type>
{
    public UInt16Consumer(int columnIndex) : base(columnIndex, UInt16Type.Default, new UInt16Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append((ushort)reader.GetInt16(ColumnIndex));
}

public sealed class UInt32Consumer : BaseAdoConsumer<UInt32Array.Builder, UInt32Array, UInt32Type>
{
    public UInt32Consumer(int columnIndex) : base(columnIndex, UInt32Type.Default, new UInt32Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append((uint)reader.GetInt32(ColumnIndex));
}

public sealed class UInt64Consumer : BaseAdoConsumer<UInt64Array.Builder, UInt64Array, UInt64Type>
{
    public UInt64Consumer(int columnIndex) : base(columnIndex, UInt64Type.Default, new UInt64Array.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append((ulong)reader.GetInt64(ColumnIndex));
}

public sealed class FloatConsumer : BaseAdoConsumer<FloatArray.Builder, FloatArray, FloatType>
{
    public FloatConsumer(int columnIndex) : base(columnIndex, FloatType.Default, new FloatArray.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append(reader.GetFloat(ColumnIndex));
}

public sealed class DoubleConsumer : BaseAdoConsumer<DoubleArray.Builder, DoubleArray, DoubleType>
{
    public DoubleConsumer(int columnIndex) : base(columnIndex, DoubleType.Default, new DoubleArray.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append(reader.GetDouble(ColumnIndex));
}

public sealed class BooleanConsumer : BaseAdoConsumer<BooleanArray.Builder, BooleanArray, BooleanType>
{
    public BooleanConsumer(int columnIndex) : base(columnIndex, BooleanType.Default, new BooleanArray.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append(reader.GetBoolean(ColumnIndex));
}

public sealed class StringConsumer : BaseAdoConsumer<StringArray.Builder, StringArray, StringType>
{
    public StringConsumer(int columnIndex) : base(columnIndex, StringType.Default, new StringArray.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader) => Builder.Append(reader.GetString(ColumnIndex));
}

public sealed class BinaryConsumer : BaseAdoConsumer<BinaryArray.Builder, BinaryArray, BinaryType>
{
    public BinaryConsumer(int columnIndex) : base(columnIndex, BinaryType.Default, new BinaryArray.Builder()) { }
    protected override void ConsumeValue(DbDataReader reader)
    {
        var obj = reader.GetValue(ColumnIndex);
        if (obj is byte[] bytes)
            Builder.Append(bytes);
        else
            Builder.AppendNull();
    }
}
