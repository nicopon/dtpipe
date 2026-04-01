using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado.Binder;

/// <summary>Boolean (bit) Arrow → DbParameter binder.</summary>
public sealed class BooleanBinder : BaseArrowBinder<BooleanArray>
{
    public BooleanBinder() : base(BooleanType.Default, (int)System.Data.DbType.Boolean) { }
    protected override void BindValue(BooleanArray a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>Int8 (sbyte) Arrow → DbParameter binder.</summary>
public sealed class Int8Binder : BaseArrowBinder<Int8Array>
{
    public Int8Binder() : base(Int8Type.Default, (int)System.Data.DbType.SByte) { }
    protected override void BindValue(Int8Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>Int16 Arrow → DbParameter binder.</summary>
public sealed class Int16Binder : BaseArrowBinder<Int16Array>
{
    public Int16Binder() : base(Int16Type.Default, (int)System.Data.DbType.Int16) { }
    protected override void BindValue(Int16Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>Int32 Arrow → DbParameter binder.</summary>
public sealed class Int32Binder : BaseArrowBinder<Int32Array>
{
    public Int32Binder() : base(Int32Type.Default, (int)System.Data.DbType.Int32) { }
    protected override void BindValue(Int32Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>Int64 Arrow → DbParameter binder.</summary>
public sealed class Int64Binder : BaseArrowBinder<Int64Array>
{
    public Int64Binder() : base(Int64Type.Default, (int)System.Data.DbType.Int64) { }
    protected override void BindValue(Int64Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>UInt8 (byte) Arrow → DbParameter binder.</summary>
public sealed class UInt8Binder : BaseArrowBinder<UInt8Array>
{
    public UInt8Binder() : base(UInt8Type.Default, (int)System.Data.DbType.Byte) { }
    protected override void BindValue(UInt8Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>UInt16 Arrow → DbParameter binder.</summary>
public sealed class UInt16Binder : BaseArrowBinder<UInt16Array>
{
    public UInt16Binder() : base(UInt16Type.Default, (int)System.Data.DbType.UInt16) { }
    protected override void BindValue(UInt16Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>UInt32 Arrow → DbParameter binder.</summary>
public sealed class UInt32Binder : BaseArrowBinder<UInt32Array>
{
    public UInt32Binder() : base(UInt32Type.Default, (int)System.Data.DbType.UInt32) { }
    protected override void BindValue(UInt32Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>UInt64 Arrow → DbParameter binder.</summary>
public sealed class UInt64Binder : BaseArrowBinder<UInt64Array>
{
    public UInt64Binder() : base(UInt64Type.Default, (int)System.Data.DbType.UInt64) { }
    protected override void BindValue(UInt64Array a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>Float32 Arrow → DbParameter binder.</summary>
public sealed class FloatBinder : BaseArrowBinder<FloatArray>
{
    public FloatBinder() : base(FloatType.Default, (int)System.Data.DbType.Single) { }
    protected override void BindValue(FloatArray a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>Float64 (double) Arrow → DbParameter binder.</summary>
public sealed class DoubleBinder : BaseArrowBinder<DoubleArray>
{
    public DoubleBinder() : base(DoubleType.Default, (int)System.Data.DbType.Double) { }
    protected override void BindValue(DoubleArray a, int row, DbParameter p)
        => p.Value = a.GetValue(row)!.Value;
}

/// <summary>UTF-8 string Arrow → DbParameter binder.</summary>
public sealed class StringBinder : BaseArrowBinder<StringArray>
{
    public StringBinder() : base(StringType.Default, (int)System.Data.DbType.String) { }
    protected override void BindValue(StringArray a, int row, DbParameter p)
        => p.Value = a.GetString(row);
}

/// <summary>Binary Arrow → DbParameter binder. Sets Value to byte[].</summary>
public sealed class BinaryBinder : BaseArrowBinder<BinaryArray>
{
    public BinaryBinder() : base(BinaryType.Default, (int)System.Data.DbType.Binary) { }
    protected override void BindValue(BinaryArray a, int row, DbParameter p)
        => p.Value = a.GetBytes(row).ToArray();
}
