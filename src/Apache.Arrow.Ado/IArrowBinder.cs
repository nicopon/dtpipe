using System;
using System.Data.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// Extracts a typed value at a given row index from an Arrow array and writes it to a
/// <see cref="DbParameter"/>. Sets <see cref="DbParameter.Value"/> to
/// <see cref="DBNull.Value"/> when the cell is null.
///
/// Symmetric counterpart to <see cref="IAdoConsumer"/> (which converts in the
/// opposite direction: <see cref="System.Data.Common.DbDataReader"/> → Arrow array builder).
/// </summary>
public interface IArrowBinder : IDisposable
{
    /// <summary>Arrow type handled by this binder.</summary>
    IArrowType ArrowType { get; }

    /// <summary>
    /// ADO.NET <see cref="System.Data.DbType"/> code used when binding null values.
    /// </summary>
    int DbType { get; }

    /// <summary>
    /// Binds the value at <paramref name="rowIndex"/> from <paramref name="array"/>
    /// into <paramref name="parameter"/>.
    /// </summary>
    void Bind(IArrowArray array, int rowIndex, DbParameter parameter);
}
