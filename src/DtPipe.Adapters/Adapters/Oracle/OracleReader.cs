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
using Oracle.ManagedDataAccess.Client;

namespace DtPipe.Adapters.Oracle;

/// <summary>
/// Columnar stream reader for Oracle. Produces Apache Arrow RecordBatches directly
/// from OracleDataReader via typed column consumers (no boxing).
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed class OracleReader : AdoColumnarReader, IRequiresOptions<OracleReaderOptions>
{
    public OracleReader(string connectionString, string query, OracleReaderOptions options, int queryTimeout = 0)
    {
        ValidateQueryIsSafeSelect(query, "FLASHBACK", "PURGE", "CALL", "LOCK", "EXPLAIN");
        Connection = new OracleConnection(connectionString);
        Command = new OracleCommand(query, (OracleConnection)Connection)
        {
            FetchSize = options.FetchSize,
            CommandTimeout = queryTimeout
        };
    }

    public override async Task OpenAsync(CancellationToken ct = default)
    {
        await Connection!.OpenAsync(ct);

        Reader = await Command!.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        Columns = ExtractColumns((OracleDataReader)Reader);

        // Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
        Schema = ArrowSchemaFactory.Create(Columns);

        Config = new AdoToArrowConfigBuilder()
            .SetTypeResolver(col => ArrowTypeMapper.GetLogicalType(
                Nullable.GetUnderlyingType(col.DataType ?? typeof(string)) ?? col.DataType ?? typeof(string)))
            .Build();
    }

    private static List<PipeColumnInfo> ExtractColumns(OracleDataReader reader)
    {
        var columns = new List<PipeColumnInfo>(reader.FieldCount);
        var schemaTable = reader.GetSchemaTable();

        if (schemaTable is null)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                columns.Add(new PipeColumnInfo(name, reader.GetFieldType(i), true,
                    IsCaseSensitive: name != name.ToUpperInvariant()));
            }
            return columns;
        }

        foreach (DataRow row in schemaTable.Rows)
        {
            var name = row["ColumnName"]?.ToString() ?? $"Column{columns.Count}";
            var clrType = row["DataType"] as Type ?? typeof(object);
            var allowNull = row["AllowDBNull"] as bool? ?? true;
            columns.Add(new PipeColumnInfo(name, clrType, allowNull,
                IsCaseSensitive: name != name.ToUpperInvariant()));
        }

        return columns;
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
    }
}
