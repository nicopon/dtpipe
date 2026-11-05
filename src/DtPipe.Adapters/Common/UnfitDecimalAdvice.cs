using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;

namespace DtPipe.Adapters.Common;

/// <summary>
/// The refusal an ADO reader raises when a decimal value does not fit the width its column was read
/// with. It names the column and its type, never the value, and names the way out: the reader
/// converts nothing, the person casts in the source query.
/// </summary>
internal static class UnfitDecimalAdvice
{
    public static InvalidDataException Refuse(DecimalOutOfRangeException fault, Schema schema)
        => new(Compose(schema.GetFieldByIndex(fault.ColumnIndex)), fault);

    public static string Compose(Field field)
    {
        var width = field.DataType switch
        {
            Decimal128Type d => $"decimal({d.Precision}, {d.Scale})",
            Decimal256Type d => $"decimal({d.Precision}, {d.Scale})",
            var other => other.Name
        };

        var read = ArrowTypeMap.IsUndeclaredDecimal(field)
            ? $"no declared precision, read as {width}"
            : $"declared {width}";

        return $"Column '{field.Name}' ({read}) holds a value that does not fit. " +
               "Cast it in the source query, to a float (approximate) or to text (exact).";
    }
}
