using Apache.Arrow;
using Apache.Arrow.Serialization;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;

namespace DtPipe.Adapters.Common;

/// <summary>
/// The refusal an ADO reader raises for a value the driver cannot hand over or its column's Arrow type
/// cannot take, on either read path. The reader converts nothing: the way out is a cast in the source
/// query.
/// </summary>
internal static class AdoValueRefusal
{
    public static ColumnValueRefusedException Of(Field field, string sourceType, long? record, ArrowValueException? inner)
    {
        var where = record is { } number ? $"Record {number}" : "A record";
        return field.DataType switch
        {
            Decimal128Type or Decimal256Type => new ColumnValueRefusedException(
                where, field.Name, ColumnValueRefusedException.TypeOf(field), "holds a value that does not fit",
                "Cast it in the source query, to a float (approximate) or to text (exact).", inner),
            Time64Type => new ColumnValueRefusedException(
                where, field.Name, sourceType, "holds a value that is not a time of day (0 to 24 hours)",
                "Cast the column to text in the source query.", inner),
            _ => new ColumnValueRefusedException(
                where, field.Name, sourceType, "holds a value the reader cannot read",
                "Cast the column to text in the source query.", inner)
        };
    }
}
