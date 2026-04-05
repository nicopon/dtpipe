using System;
using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Ado.Consumer;

namespace Apache.Arrow.Ado;

/// <summary>
/// Default factory for creating <see cref="IAdoConsumer"/> instances from Arrow types.
/// Exposed as a public static method so callers can use it as a fallback in custom factories.
/// </summary>
public static class AdoConsumerFactory
{
    /// <summary>
    /// Creates a default <see cref="IAdoConsumer"/> for the given Arrow type and column index.
    /// </summary>
    /// <param name="arrowType">The Arrow type of the column.</param>
    /// <param name="columnIndex">The zero-based column index in the <see cref="System.Data.Common.DbDataReader"/>.</param>
    /// <returns>A consumer appropriate for the given type.</returns>
    /// <exception cref="NotSupportedException">If the Arrow type has no default consumer.</exception>
    public static IAdoConsumer Create(IArrowType arrowType, int columnIndex)
    {
        return arrowType switch
        {
            BooleanType => new BooleanConsumer(columnIndex),
            Int8Type => new Int8Consumer(columnIndex),
            Int16Type => new Int16Consumer(columnIndex),
            Int32Type => new Int32Consumer(columnIndex),
            Int64Type => new Int64Consumer(columnIndex),
            UInt8Type => new UInt8Consumer(columnIndex),
            UInt16Type => new UInt16Consumer(columnIndex),
            UInt32Type => new UInt32Consumer(columnIndex),
            UInt64Type => new UInt64Consumer(columnIndex),
            FloatType => new FloatConsumer(columnIndex),
            DoubleType => new DoubleConsumer(columnIndex),
            StringType => new StringConsumer(columnIndex),
            BinaryType => new BinaryConsumer(columnIndex),
            Date32Type => new Date32Consumer(columnIndex),
            Date64Type => new Date64Consumer(columnIndex),
            TimestampType t => new TimestampConsumer(columnIndex, t),
            DurationType => new DurationConsumer(columnIndex),
            Time32Type => new Time32Consumer(columnIndex),
            Decimal128Type t => new Decimal128Consumer(columnIndex, t),
            Decimal256Type t => new Decimal256Consumer(columnIndex, t),
            _ => throw new NotSupportedException($"No default consumer for Arrow type: {arrowType.Name}")
        };
    }
}
