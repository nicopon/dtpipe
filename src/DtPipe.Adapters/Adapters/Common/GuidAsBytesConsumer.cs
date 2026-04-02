using System;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Types;
using Apache.Arrow.Ado;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Consumer for database columns that expose <see cref="Guid"/> values (e.g. UUID/UNIQUEIDENTIFIER).
/// Produces a <c>FixedSizeBinaryArray</c> of 16-byte RFC 4122 values.
/// The Arrow field for this column will carry the <c>arrow.uuid</c> extension metadata
/// (set by <see cref="DtPipe.Core.Infrastructure.Arrow.ArrowSchemaFactory"/>).
/// Handles Guid-returning providers such as Npgsql (PostgreSQL uuid) and SqlClient (SQL Server uniqueidentifier).
/// </summary>
internal sealed class GuidAsBytesConsumer : IAdoConsumer
{
    private readonly int _columnIndex;
    private readonly DtPipe.Core.Infrastructure.Arrow.FixedSizeBinaryArrayBuilder _builder = new(16);

    public GuidAsBytesConsumer(int columnIndex)
    {
        _columnIndex = columnIndex;
    }

    public IArrowType ArrowType => new FixedSizeBinaryType(16);

    public void Consume(DbDataReader reader)
    {
        if (reader.IsDBNull(_columnIndex))
        {
            _builder.AppendNull();
            return;
        }

        var obj = reader.GetValue(_columnIndex);
        if (obj is Guid guid)
            _builder.Append(ArrowTypeMapper.ToArrowUuidBytes(guid));
        else if (obj is byte[] bytes && bytes.Length == 16)
            _builder.Append(bytes.AsSpan()); // assume already RFC 4122 from the DB driver
        else
            _builder.AppendNull();
    }

    public IArrowArray BuildArray() => _builder.Build();

    public void Reset() => _builder.Clear();

    public void Dispose() { }
}
