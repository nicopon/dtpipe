using System;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Base class for ADO.NET-based columnar stream readers.
/// Provides common implementation for row-to-Arrow fallback and query validation.
/// </summary>
public abstract partial class AdoColumnarReader : IColumnarStreamReader, IBatchSizeConfigurable
{
    protected DbConnection? Connection;
    protected DbCommand? Command;
    protected DbDataReader? Reader;
    protected AdoToArrowConfig? Config;

    public int BatchSize { get; set; } = PipelineOptions.DefaultBatchSize;
    public long MaxBatchBytes { get; set; }

    public IReadOnlyList<PipeColumnInfo>? Columns { get; protected set; }
    public Schema? Schema { get; protected set; }

    public abstract Task OpenAsync(CancellationToken ct = default);

    /// <summary>
    /// The one type resolver the ADO readers share: the CLR type and the declared decimal
    /// precision and scale come from <see cref="Columns"/> (matched by ordinal), so the Arrow
    /// batches carry the width the source declared. A decimal that declares none keeps
    /// <c>ArrowTypeMap.DefaultDecimalType</c>.
    /// </summary>
    protected Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> DeclaredTypeResolver => col =>
    {
        var declared = col.ColumnOrdinal is int i && Columns is { } columns && i >= 0 && i < columns.Count
            ? columns[i]
            : null;
        var clrType = declared?.ClrType ?? col.DataType ?? typeof(string);
        clrType = Nullable.GetUnderlyingType(clrType) ?? clrType;
        return ArrowTypeMapper.GetLogicalType(clrType, declared?.Precision, declared?.Scale);
    };

    /// <summary>
    /// Value of the current row's cell in row mode. A reader whose driver hands back a different
    /// CLR type than the one <see cref="Columns"/> declares overrides this to convert.
    /// </summary>
    protected virtual object? GetCellValue(int ordinal) => Reader!.GetValue(ordinal);

    public virtual async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (Reader is null || Schema is null)
            throw new InvalidOperationException("Call OpenAsync first.");

        await foreach (var batch in AdoToArrow.ReadToArrowBatchesAsync(Reader, Config, GetConsumerFactory(), ct))
        {
            yield return batch;
        }
    }

    public virtual async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (Reader is null) throw new InvalidOperationException("Call OpenAsync first.");

        var columnCount = Reader.FieldCount;
        var batch = new object?[batchSize][];
        var index = 0;

        while (await Reader.ReadAsync(ct))
        {
            var row = new object?[columnCount];
            for (var i = 0; i < columnCount; i++)
                row[i] = Reader.IsDBNull(i) ? null : GetCellValue(i);

            batch[index++] = row;

            if (index >= batchSize)
            {
                yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
                batch = new object?[batchSize][];
                index = 0;
            }
        }

        if (index > 0)
            yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
    }

    protected virtual Func<IArrowType, int, IAdoConsumer>? GetConsumerFactory() => null;

    public virtual async ValueTask DisposeAsync()
    {
        if (Reader is not null) { await Reader.DisposeAsync(); Reader = null; }
        if (Command is not null) { await Command.DisposeAsync(); Command = null; }
        if (Connection is not null) { await Connection.DisposeAsync(); Connection = null; }
    }

    protected static void ValidateQueryIsSafeSelect(string query, params string[] additionalAllowedKeywords)
        => SqlQueryGuard.RequireReadOnlyStatement(query, additionalAllowedKeywords);
}
