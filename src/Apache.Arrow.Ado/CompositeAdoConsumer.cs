using System;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// Orchestrates an array of <see cref="IAdoConsumer"/> instances, one per column
/// in the source <see cref="DbDataReader"/>.
/// </summary>
internal sealed class CompositeAdoConsumer : IDisposable
{
    private readonly IAdoConsumer[] _consumers;
    private readonly Schema _schema;

    /// <summary>
    /// Gets the Arrow schema used by this consumer.
    /// </summary>
    public Schema Schema => _schema;

    /// <param name="reader">The data reader (used only for schema introspection if needed).</param>
    /// <param name="mappedSchema">The Arrow schema describing the columns to consume.</param>
    /// <param name="consumerFactory">
    /// Optional factory for creating per-column consumers. When null, <see cref="AdoConsumerFactory.Create"/>
    /// is used. Inject a custom factory to handle provider-specific type quirks
    /// (e.g. a Guid-returning column mapped to BinaryType).
    /// </param>
    public CompositeAdoConsumer(DbDataReader reader, Schema mappedSchema,
        Func<IArrowType, int, IAdoConsumer>? consumerFactory = null)
    {
        _schema = mappedSchema;
        _consumers = new IAdoConsumer[_schema.FieldsList.Count];

        var factory = consumerFactory ?? AdoConsumerFactory.Create;
        for (int i = 0; i < _consumers.Length; i++)
        {
            var field = _schema.GetFieldByIndex(i);
            _consumers[i] = factory(field.DataType, i);
        }
    }

    public void ConsumeRow(DbDataReader reader)
    {
        for (int i = 0; i < _consumers.Length; i++)
        {
            _consumers[i].Consume(reader);
        }
    }

    public RecordBatch BuildBatch(int rowCount)
    {
        var arrays = new IArrowArray[_consumers.Length];
        for (int i = 0; i < _consumers.Length; i++)
        {
            arrays[i] = _consumers[i].BuildArray();
        }
        return new RecordBatch(_schema, arrays, rowCount);
    }

    public void Reset()
    {
        for (int i = 0; i < _consumers.Length; i++)
        {
            _consumers[i].Reset();
        }
    }

    public void Dispose()
    {
        for (int i = 0; i < _consumers.Length; i++)
        {
            _consumers[i].Dispose();
        }
    }
}
