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
        var columns = new List<PipeColumnInfo>(Reader.FieldCount);
        for (int i = 0; i < Reader.FieldCount; i++)
        {
            columns.Add(new PipeColumnInfo(
                Reader.GetName(i),
                Reader.GetFieldType(i),
                true,
                IsCaseSensitive: false));
        }
        Columns = columns;

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);

        // SQLite CLR types: long, double, string, byte[] — no special consumers needed
        Config = new AdoToArrowConfigBuilder()
            .SetTypeResolver(col => ArrowTypeMapper.GetLogicalType(
                Nullable.GetUnderlyingType(col.DataType ?? typeof(string)) ?? col.DataType ?? typeof(string)))
            .Build();
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
