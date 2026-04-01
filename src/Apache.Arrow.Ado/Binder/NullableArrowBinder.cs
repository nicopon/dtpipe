using System.Data.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado.Binder;

/// <summary>
/// Decorator that overrides the null-binding behaviour of a wrapped <see cref="IArrowBinder"/>.
/// Useful when the default null check in <see cref="BaseArrowBinder{TArray}"/> needs to be
/// bypassed or the DbType for null needs to be overridden.
/// Analogous to <c>NullableColumnBinder</c> in the Java arrow-jdbc adapter.
/// </summary>
public sealed class NullableArrowBinder : IArrowBinder
{
    private readonly IArrowBinder _inner;
    private readonly int _nullDbType;

    public IArrowType ArrowType => _inner.ArrowType;
    public int DbType => _nullDbType;

    /// <param name="inner">The binder to delegate non-null rows to.</param>
    /// <param name="nullDbType">
    /// The <see cref="System.Data.DbType"/> code to use when the value is null.
    /// Defaults to the inner binder's DbType.
    /// </param>
    public NullableArrowBinder(IArrowBinder inner, int? nullDbType = null)
    {
        _inner = inner;
        _nullDbType = nullDbType ?? inner.DbType;
    }

    public void Bind(IArrowArray array, int rowIndex, DbParameter parameter)
    {
        if (array.IsNull(rowIndex))
        {
            parameter.Value = System.DBNull.Value;
            if (parameter is System.Data.Common.DbParameter dbp)
                dbp.DbType = (System.Data.DbType)_nullDbType;
        }
        else
        {
            _inner.Bind(array, rowIndex, parameter);
        }
    }

    public void Dispose() => _inner.Dispose();
}
