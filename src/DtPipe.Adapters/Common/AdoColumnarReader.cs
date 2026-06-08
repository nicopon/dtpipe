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
using DtPipe.Core.Models;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Base class for ADO.NET-based columnar stream readers.
/// Provides common implementation for row-to-Arrow fallback and query validation.
/// </summary>
public abstract partial class AdoColumnarReader : IColumnarStreamReader
{
    protected DbConnection? Connection;
    protected DbCommand? Command;
    protected DbDataReader? Reader;
    protected AdoToArrowConfig? Config;

    public IReadOnlyList<PipeColumnInfo>? Columns { get; protected set; }
    public Schema? Schema { get; protected set; }

    [GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
    private static partial Regex FirstWordRegex();

    public abstract Task OpenAsync(CancellationToken ct = default);

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
                row[i] = Reader.IsDBNull(i) ? null : Reader.GetValue(i);

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
    {
        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentException("Query cannot be empty.", nameof(query));

        var match = FirstWordRegex().Match(query);
        if (!match.Success)
            throw new ArgumentException("Invalid query format.", nameof(query));

        var firstWord = match.Groups[1].Value.ToUpperInvariant();
        var isAllowed = firstWord == "SELECT" || firstWord == "WITH" || additionalAllowedKeywords.Contains(firstWord, StringComparer.OrdinalIgnoreCase);

        if (!isAllowed)
            throw new InvalidOperationException(
                $"Only SELECT/WITH queries are allowed. Detected: {firstWord}. DDL/DML statements are blocked for safety.");
    }
}
