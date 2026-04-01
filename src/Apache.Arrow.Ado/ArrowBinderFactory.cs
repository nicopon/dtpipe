using System;
using Apache.Arrow.Ado.Binder;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// Default factory for creating <see cref="IArrowBinder"/> instances from Arrow types.
/// Symmetric counterpart to <see cref="AdoConsumerFactory"/>.
///
/// Exposed as a public static method so callers can use it as a fallback in custom factories.
/// </summary>
public static class ArrowBinderFactory
{
    /// <summary>
    /// Creates a default <see cref="IArrowBinder"/> for the given Arrow type.
    /// </summary>
    /// <param name="arrowType">The Arrow type of the column to bind.</param>
    /// <returns>A binder appropriate for the given type.</returns>
    /// <exception cref="NotSupportedException">If the Arrow type has no default binder.</exception>
    public static IArrowBinder Create(IArrowType arrowType) => arrowType switch
    {
        BooleanType   => new BooleanBinder(),
        Int8Type      => new Int8Binder(),
        Int16Type     => new Int16Binder(),
        Int32Type     => new Int32Binder(),
        Int64Type     => new Int64Binder(),
        UInt8Type     => new UInt8Binder(),
        UInt16Type    => new UInt16Binder(),
        UInt32Type    => new UInt32Binder(),
        UInt64Type    => new UInt64Binder(),
        FloatType     => new FloatBinder(),
        DoubleType    => new DoubleBinder(),
        StringType    => new StringBinder(),
        BinaryType    => new BinaryBinder(),
        Date32Type    => new Date32Binder(),
        Date64Type    => new Date64Binder(),
        TimestampType => new TimestampBinder(),
        DurationType  => new DurationBinder(),
        Decimal128Type t => new Decimal128Binder(t),
        Decimal256Type t => new Decimal256Binder(t),
        _ => throw new NotSupportedException(
            $"No default binder for Arrow type: {arrowType.Name}. " +
            "Provide a custom factory to handle this type.")
    };
}
