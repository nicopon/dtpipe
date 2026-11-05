using Apache.Arrow;
using Apache.Arrow.Serialization;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// One value a reader or a writer cannot carry, raised at the cell, where
/// <see cref="ColumnReadRefusedException"/> answers for a type when the reader opens. It stops the run
/// naming where the value stands, the column, its type as the source spells it and the way out; every
/// refusal of a value takes this form, so a person never meets a driver's or a builder's message
/// without the column. The value is not quoted: a refused cell can hold anything the source holds.
/// It stays an <see cref="InvalidOperationException"/>, as the text readers' refusals are to a host
/// that catches them.
/// <see cref="Exception.Message"/> is plain text the host formats.
/// </summary>
/// <param name="where">The record, as the source counts it (<c>CSV line 3</c>, <c>Record 12</c>).</param>
/// <param name="column">The column, with the path to the field for a value inside a struct or a list.</param>
/// <param name="sourceType">The column's type as the source or the declaration spells it.</param>
/// <param name="problem">What the value is, following the column: <c>holds infinity, …</c>.</param>
/// <param name="remedy">The way out, a sentence or more.</param>
/// <param name="inner">The fault this explains, when its message quotes no value.</param>
public sealed class ColumnValueRefusedException(
    string where, string column, string sourceType, string problem, string remedy, Exception? inner = null)
    : InvalidOperationException($"{where}: column '{column}' ({sourceType}) {problem}. {remedy}", inner)
{
    public string Where { get; } = where;
    public string Column { get; } = column;
    public string SourceType { get; } = sourceType;
    public string Remedy { get; } = remedy;

    /// <summary>
    /// The refusal of a value a row carried and its column's Arrow type cannot take. The fault explains
    /// it and quotes no value.
    /// </summary>
    public static ColumnValueRefusedException Unfit(ArrowValueException fault, Schema schema, string where, string remedy)
    {
        var field = schema.FieldsList.FirstOrDefault(f => f.Name == fault.Column);
        return new ColumnValueRefusedException(
            where,
            fault.Column,
            field != null ? TypeOf(field) : ArrowTypeMap.Spell(fault.ArrowType),
            "holds a value that does not fit",
            remedy,
            fault);
    }

    /// <summary>
    /// A field's type, for a source that spells none: a decimal says whether its width was declared,
    /// since one that declared none reads under a default that a value can exceed.
    /// </summary>
    public static string TypeOf(Field field) => field.DataType switch
    {
        Decimal128Type or Decimal256Type when ArrowTypeMap.IsUndeclaredDecimal(field)
            => $"no declared precision, read as {ArrowTypeMap.Spell(field.DataType)}",
        Decimal128Type or Decimal256Type => $"declared {ArrowTypeMap.Spell(field.DataType)}",
        _ => ArrowTypeMap.Spell(field.DataType)
    };
}
