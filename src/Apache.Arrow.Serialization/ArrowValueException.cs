using System;
using System.IO;
using Apache.Arrow.Types;

namespace Apache.Arrow.Serialization;

/// <summary>
/// A value a column's Arrow type cannot take, or a driver cannot hand over, raised where a row meets
/// its builders. It carries the column, its type and the record, never the value: the original fault
/// is not chained, since the messages of Arrow and of the drivers quote the number, and only its type
/// name is kept.
/// </summary>
public sealed class ArrowValueException : Exception
{
    public ArrowValueException(string column, IArrowType arrowType, string causeTypeName, long? record = null)
        : base($"A value of column '{column}' does not fit {arrowType.Name} ({causeTypeName}).")
    {
        Column = column;
        ArrowType = arrowType;
        CauseTypeName = causeTypeName;
        Record = record;
    }

    /// <summary>The column, with the path to the field for a value inside a struct.</summary>
    public string Column { get; }

    /// <summary>The Arrow type the value was read into.</summary>
    public IArrowType ArrowType { get; }

    /// <summary>Type name of the fault that was dropped.</summary>
    public string CauseTypeName { get; }

    /// <summary>One-based position of the record among those the raiser was given, when it knows it.</summary>
    public long? Record { get; }

    /// <summary>The same fault one struct further out, its column prefixed with the parent's name.</summary>
    public ArrowValueException Under(string parent)
        => new($"{parent}.{Column}", ArrowType, CauseTypeName, Record);

    /// <summary>The same fault, at this record.</summary>
    public ArrowValueException AtRecord(long record)
        => new(Column, ArrowType, CauseTypeName, record);

    /// <summary>
    /// Whether a fault raised while appending a cell is about the value: a builder or a driver that
    /// cannot take it. A connection or an I/O fault is not, and keeps its own type.
    /// </summary>
    public static bool IsValueFault(Exception fault)
        => fault is InvalidCastException or OverflowException or FormatException
            or ArgumentOutOfRangeException or InvalidDataException;
}
