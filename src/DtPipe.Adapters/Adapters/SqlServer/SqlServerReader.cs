using System.Data;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using DtPipe.Adapters.Common;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Data.SqlClient;

namespace DtPipe.Adapters.SqlServer;

/// <summary>
/// Columnar stream reader for SQL Server. Produces Apache Arrow RecordBatches directly
/// from SqlDataReader via typed column consumers (no boxing).
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed class SqlServerReader : AdoColumnarReader, IRequiresOptions<SqlServerReaderOptions>
{
    private Func<IArrowType, int, IAdoConsumer>? _consumerFactory;
    private SqlTransaction? _transaction;

    public SqlServerReader(string connectionString, string query, SqlServerReaderOptions options, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query, "EXEC", "EXECUTE");
        Connection = new SqlConnection(connectionString);
        Command = new SqlCommand(query, (SqlConnection)Connection) { CommandTimeout = queryTimeout };
    }

    /// <summary>
    /// Whether the connected database allows snapshot isolation. Anything but a definite ON —
    /// OFF, either transition state, or a row the caller cannot read — answers no, because
    /// ReadCommitted is the level that always works.
    /// </summary>
    private async Task<bool> SupportsSnapshotAsync(CancellationToken ct)
    {
        await using var probe = new SqlCommand(
            "SELECT snapshot_isolation_state FROM sys.databases WHERE database_id = DB_ID()",
            (SqlConnection)Connection!);

        var state = await probe.ExecuteScalarAsync(ct);
        return state is byte on && on == 1;
    }

    public override async Task OpenAsync(CancellationToken ct = default)
    {
        await Connection!.OpenAsync(ct);

        // Snapshot gives a consistent read without blocking writers, but the database has to
        // allow it and a freshly created one does not — ALLOW_SNAPSHOT_ISOLATION is OFF by
        // default. Ask before choosing: SQL Server raises error 3952 when a statement first
        // touches a user object, not when the transaction opens, so catching around
        // BeginTransaction guards a line the error never reaches. One extra round trip buys a
        // level the server has already agreed to.
        var level = await SupportsSnapshotAsync(ct) ? IsolationLevel.Snapshot : IsolationLevel.ReadCommitted;
        _transaction = ((SqlConnection)Connection).BeginTransaction(level);
        Command!.Transaction = _transaction;

        Reader = await Command!.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);

        var dbColumns = ((SqlDataReader)Reader).GetColumnSchema();

        // Build PipeColumnInfo from DB schema (CLR types) — authoritative for DtPipe pipeline
        var columns = new List<PipeColumnInfo>(dbColumns.Count);
        foreach (var col in dbColumns)
        {
            columns.Add(new PipeColumnInfo(
                col.ColumnName,
                col.DataType ?? typeof(object),
                col.AllowDBNull ?? true,
                IsCaseSensitive: false, // SQL Server is case-insensitive by default
                Precision: (int?)col.NumericPrecision,
                Scale: (int?)col.NumericScale
            ));
        }
        Columns = columns;

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);

        // Build consumer factory with DtPipe type semantics (handles Guid → BinaryType)
        var guidColumnIndexes = new HashSet<int>(
            dbColumns.Select((c, i) => (c, i))
                     .Where(x => x.c.DataType == typeof(Guid))
                     .Select(x => x.i));

        Config = new AdoToArrowConfigBuilder()
            .SetTypeResolver(col => ArrowTypeMapper.GetLogicalType(
                Nullable.GetUnderlyingType(col.DataType ?? typeof(string)) ?? col.DataType ?? typeof(string)))
            .SetTargetBatchSize(BatchSize)
            .SetMaxBatchBytes(MaxBatchBytes)
            .Build();

        _consumerFactory = (arrowType, colIdx) =>
            guidColumnIndexes.Contains(colIdx)
                ? new GuidAsBytesConsumer(colIdx)
                : AdoConsumerFactory.Create(arrowType, colIdx);
    }

    protected override Func<IArrowType, int, IAdoConsumer>? GetConsumerFactory() => _consumerFactory;

    public override async ValueTask DisposeAsync()
    {
        if (Reader is not null)
        {
            await Reader.DisposeAsync();
            Reader = null;
        }

        if (_transaction is not null)
        {
            try { await _transaction.RollbackAsync(); } catch { }
            await _transaction.DisposeAsync();
            _transaction = null;
        }
        await base.DisposeAsync();
    }
}
