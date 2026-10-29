using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Adapters.Shared.Infrastructure;

/// <summary>Building blocks of <see cref="IColumnTypeGate.RefuseColumns"/> that several writers share.</summary>
public static class ColumnTypeRefusals
{
    /// <summary>
    /// One refusal per field for which <paramref name="acceptedInstead"/> answers; a null answer
    /// means the writer carries the field as it is.
    /// </summary>
    public static IReadOnlyList<ColumnTypeRefusal> Collect(Schema schema, Func<Field, IReadOnlyList<IArrowType>?> acceptedInstead)
    {
        List<ColumnTypeRefusal>? refusals = null;
        foreach (var field in schema.FieldsList)
        {
            if (acceptedInstead(field) is { } accepted)
                (refusals ??= []).Add(new ColumnTypeRefusal(field, accepted));
        }
        return refusals ?? (IReadOnlyList<ColumnTypeRefusal>)[];
    }

    /// <summary>
    /// What a 128-bit integer (DuckDB's HUGEINT, the <c>arrow.opaque</c> field of an <see cref="Int128"/>)
    /// is accepted as by a writer with no type that holds every value: its text, the one form that keeps
    /// all 39 digits; null for any other field. The writer never spells the number itself.
    /// </summary>
    public static IReadOnlyList<IArrowType>? WideInteger(Field field)
        => ArrowTypeMapper.GetClrTypeFromField(field) is var t && (t == typeof(Int128) || t == typeof(UInt128))
            ? [StringType.Default]
            : null;

    /// <summary>
    /// The narrowest signed type that holds every value of an integer width a format lacks, for the
    /// person who chooses to cast to it. The writer never casts: it names the type.
    /// </summary>
    public static IReadOnlyList<IArrowType> SignedHolding(IArrowType integer) => integer switch
    {
        Int8Type or UInt8Type => [Int16Type.Default],
        UInt16Type => [Int32Type.Default],
        UInt32Type => [Int64Type.Default],
        UInt64Type => [new Decimal128Type(20, 0)],
        _ => []
    };
}
