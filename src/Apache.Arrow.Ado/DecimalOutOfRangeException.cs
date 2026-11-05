using System;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// A decimal column's value does not fit the Arrow decimal type the column was declared with, or
/// the driver cannot hand it over as a <see cref="decimal"/>. It carries the column, never the
/// value: the original fault is not chained, since the messages of Arrow and of the drivers quote
/// the number, and only its type name is kept.
/// </summary>
public sealed class DecimalOutOfRangeException : Exception
{
    public DecimalOutOfRangeException(int columnIndex, IArrowType arrowType, Exception cause)
        : base($"A value of column {columnIndex} does not fit {arrowType.Name} ({cause.GetType().Name}).")
    {
        ColumnIndex = columnIndex;
        ArrowType = arrowType;
        CauseTypeName = cause.GetType().Name;
    }

    /// <summary>Zero-based position of the column in the reader.</summary>
    public int ColumnIndex { get; }

    /// <summary>The Arrow decimal type the column was declared with.</summary>
    public IArrowType ArrowType { get; }

    /// <summary>Type name of the fault that was dropped.</summary>
    public string CauseTypeName { get; }
}
