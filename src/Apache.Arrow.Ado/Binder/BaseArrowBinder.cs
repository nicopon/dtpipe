using System;
using System.Data.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado.Binder;

/// <summary>
/// Abstract base for <see cref="IArrowBinder"/> implementations.
/// Handles null checking before delegating to the typed <see cref="BindValue"/> override.
/// Analogous to <c>BaseAdoConsumer</c> on the read side.
/// </summary>
/// <typeparam name="TArray">The concrete Arrow array type this binder reads from.</typeparam>
public abstract class BaseArrowBinder<TArray> : IArrowBinder
    where TArray : IArrowArray
{
    private readonly IArrowType _arrowType;

    public IArrowType ArrowType => _arrowType;
    public int DbType { get; }

    protected BaseArrowBinder(IArrowType arrowType, int dbType)
    {
        _arrowType = arrowType;
        DbType = dbType;
    }

    /// <inheritdoc/>
    public void Bind(IArrowArray array, int rowIndex, DbParameter parameter)
    {
        if (array.IsNull(rowIndex))
            parameter.Value = DBNull.Value;
        else
            BindValue((TArray)array, rowIndex, parameter);
    }

    /// <summary>
    /// Binds a non-null value from <paramref name="array"/> at <paramref name="rowIndex"/>
    /// to <paramref name="parameter"/>. Null check is already handled by the base class.
    /// </summary>
    protected abstract void BindValue(TArray array, int rowIndex, DbParameter parameter);

    public virtual void Dispose() { }
}
