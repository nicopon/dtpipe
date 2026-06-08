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

    public override async Task OpenAsync(CancellationToken ct = default)
    {
        await Connection!.OpenAsync(ct);

        // Try to use Snapshot isolation for consistent reads without blocking, 
        // fall back to ReadCommitted if Snapshot is not enabled on the database
        try
        {
            _transaction = ((SqlConnection)Connection).BeginTransaction(IsolationLevel.Snapshot);
            Command!.Transaction = _transaction;
        }
        catch (SqlException)
        {
            _transaction = ((SqlConnection)Connection).BeginTransaction(IsolationLevel.ReadCommitted);
            Command!.Transaction = _transaction;
        }

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
                IsCaseSensitive: false // SQL Server is case-insensitive by default
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
