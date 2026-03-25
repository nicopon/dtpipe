using System;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Ado;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Consumer for database columns that expose <see cref="Guid"/> values (e.g. UUID/UNIQUEIDENTIFIER)
/// when the target Arrow type is <see cref="BinaryType"/> (16-byte binary representation).
/// Standard BinaryConsumer expects byte[] from the reader; this handles Guid-returning providers
/// such as Npgsql (PostgreSQL uuid) and SqlClient (SQL Server uniqueidentifier).
/// </summary>
internal sealed class GuidAsBytesConsumer : IAdoConsumer
{
    private readonly int _columnIndex;
    private readonly BinaryArray.Builder _builder = new();

    public GuidAsBytesConsumer(int columnIndex)
    {
        _columnIndex = columnIndex;
    }

    public IArrowType ArrowType => BinaryType.Default;

    public void Consume(DbDataReader reader)
    {
        if (reader.IsDBNull(_columnIndex))
        {
            _builder.AppendNull();
            return;
        }

        var obj = reader.GetValue(_columnIndex);
        if (obj is Guid guid)
            _builder.Append(guid.ToByteArray());
        else if (obj is byte[] bytes)
            _builder.Append(bytes);
        else
            _builder.AppendNull();
    }

    public IArrowArray BuildArray() => _builder.Build();

    public void Reset() => _builder.Clear();

    public void Dispose() { }
}
