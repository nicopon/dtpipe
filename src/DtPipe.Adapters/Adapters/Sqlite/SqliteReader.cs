using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using DtPipe.Adapters.Common;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Microsoft.Data.Sqlite;

namespace DtPipe.Adapters.Sqlite;

/// <summary>
/// Columnar stream reader for SQLite. Produces Apache Arrow RecordBatches directly
/// from SqliteDataReader via typed column consumers (no boxing).
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed class SqliteReader : AdoColumnarReader
{
    private readonly string _connectionString;
    private readonly string _query;
    private readonly int _queryTimeout;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public SqliteReader(string connectionString, string query, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query, "VALUES", "PRAGMA");

        // Enforce ReadOnly mode
        var csb = new SqliteConnectionStringBuilder(connectionString) { Mode = SqliteOpenMode.ReadOnly };
        _connectionString = csb.ConnectionString;

        _query = query;
        _queryTimeout = queryTimeout;
    }

    public override async Task OpenAsync(CancellationToken ct = default)
    {
        Connection = new SqliteConnection(_connectionString);
        await Connection.OpenAsync(ct);

        Command = Connection.CreateCommand();
        Command.CommandText = _query;
        if (_queryTimeout > 0) Command.CommandTimeout = _queryTimeout;

        Reader = await Command.ExecuteReaderAsync(ct);

        // SQLite is dynamically typed — use GetFieldType(i) directly, assume all nullable
        var clrTypes = new Type[Reader.FieldCount];
        for (int i = 0; i < clrTypes.Length; i++)
            clrTypes[i] = Reader.GetFieldType(i);
        await ResolveColumnsTypedByANullAsync(clrTypes, ct);

        var columns = new List<PipeColumnInfo>(clrTypes.Length);
        for (int i = 0; i < clrTypes.Length; i++)
        {
            columns.Add(new PipeColumnInfo(
                Reader.GetName(i),
                clrTypes[i],
                true,
                IsCaseSensitive: false));
        }
        Columns = columns;

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);

        // SQLite CLR types: long, double, string, byte[] — no special consumers needed
        Config = new AdoToArrowConfigBuilder()
            .SetTypeResolver(DeclaredTypeResolver)
            .SetTargetBatchSize(BatchSize)
            .SetMaxBatchBytes(MaxBatchBytes)
            .Build();
    }

    /// <summary>Rows the probe reads at most to find the first value of a column the driver typed as a blob.</summary>
    private const int ProbeRows = 256;

    /// <summary>
    /// SQLite types a value, never a column: a column with no declared type (an expression, a view
    /// column, a table made with a bare name) reports the type of the first row it meets, and the
    /// driver reports a NULL there as a blob. The rows after it then arrive as text or numbers into
    /// a binary column. A column reported as a blob is therefore typed by its first value that is
    /// not NULL, read from a second pass over the same query that stops after
    /// <see cref="ProbeRows"/> rows. A column holding only NULLs and blobs in that window stays
    /// binary, and a value that later does not fit is refused by name.
    /// </summary>
    private async Task ResolveColumnsTypedByANullAsync(Type[] clrTypes, CancellationToken ct)
    {
        var pending = Enumerable.Range(0, clrTypes.Length).Where(i => clrTypes[i] == typeof(byte[])).ToList();
        if (pending.Count == 0) return;

        await using var probe = Connection!.CreateCommand();
        probe.CommandText = _query;
        if (_queryTimeout > 0) probe.CommandTimeout = _queryTimeout;
        await using var rows = await probe.ExecuteReaderAsync(ct);

        for (var row = 0; row < ProbeRows && pending.Count > 0 && await rows.ReadAsync(ct); row++)
        {
            for (var k = pending.Count - 1; k >= 0; k--)
            {
                var ordinal = pending[k];
                if (await rows.IsDBNullAsync(ordinal, ct)) continue;

                clrTypes[ordinal] = rows.GetFieldType(ordinal);
                pending.RemoveAt(k);
            }
        }
    }

    public override async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await _semaphore.WaitAsync(ct);
        try
        {
            await foreach (var batch in base.ReadRecordBatchesAsync(ct))
            {
                yield return batch;
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public override async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await _semaphore.WaitAsync(ct);
        try
        {
            await foreach (var batch in base.ReadBatchesAsync(batchSize, ct))
            {
                yield return batch;
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public override async ValueTask DisposeAsync()
    {
        await _semaphore.WaitAsync();
        try
        {
            await base.DisposeAsync();
        }
        finally
        {
            _semaphore.Release();
            _semaphore.Dispose();
        }
    }
}
