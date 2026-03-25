using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// Provides methods to convert ADO.NET <see cref="DbDataReader"/> results into Apache Arrow <see cref="RecordBatch"/>es.
/// </summary>
public static class AdoToArrow
{
    /// <summary>
    /// Reads data from a <see cref="DbDataReader"/> and converts it into a sequence of <see cref="RecordBatch"/>es asynchronously.
    /// </summary>
    /// <param name="reader">The open data reader to read from. The reader will NOT be closed by this method.</param>
    /// <param name="config">Optional configuration. If null, defaults are used.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable sequence of RecordBatches.</returns>
    public static async IAsyncEnumerable<RecordBatch> ReadToArrowBatchesAsync(
        DbDataReader reader,
        AdoToArrowConfig? config = null,
        Func<IArrowType, int, IAdoConsumer>? consumerFactory = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (reader == null) throw new ArgumentNullException(nameof(reader));

        config ??= new AdoToArrowConfigBuilder().Build();

        var schema = AdoToArrowUtils.CreateSchema(reader, config);
        using var compositeConsumer = new CompositeAdoConsumer(reader, schema, consumerFactory);

        int rowsInBuffer = 0;

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            compositeConsumer.ConsumeRow(reader);
            rowsInBuffer++;

            if (rowsInBuffer >= config.TargetBatchSize)
            {
                yield return compositeConsumer.BuildBatch(rowsInBuffer);
                compositeConsumer.Reset();
                rowsInBuffer = 0;
            }
        }

        if (rowsInBuffer > 0)
        {
            yield return compositeConsumer.BuildBatch(rowsInBuffer);
        }
    }

    /// <summary>
    /// Reads data from a <see cref="DbDataReader"/> and converts it into a sequence of <see cref="RecordBatch"/>es synchronously.
    /// </summary>
    public static IEnumerable<RecordBatch> ReadToArrowBatches(
        DbDataReader reader,
        AdoToArrowConfig? config = null,
        Func<IArrowType, int, IAdoConsumer>? consumerFactory = null)
    {
        if (reader == null) throw new ArgumentNullException(nameof(reader));

        config ??= new AdoToArrowConfigBuilder().Build();

        var schema = AdoToArrowUtils.CreateSchema(reader, config);
        using var compositeConsumer = new CompositeAdoConsumer(reader, schema, consumerFactory);

        int rowsInBuffer = 0;

        while (reader.Read())
        {
            compositeConsumer.ConsumeRow(reader);
            rowsInBuffer++;

            if (rowsInBuffer >= config.TargetBatchSize)
            {
                yield return compositeConsumer.BuildBatch(rowsInBuffer);
                compositeConsumer.Reset();
                rowsInBuffer = 0;
            }
        }

        if (rowsInBuffer > 0)
        {
            yield return compositeConsumer.BuildBatch(rowsInBuffer);
        }
    }
}
