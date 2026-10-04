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
            .SetTypeResolver(DeclaredTypeResolver)
            .SetTargetBatchSize(BatchSize)
            .SetMaxBatchBytes(MaxBatchBytes)
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
            var (precision, scale) = DeclaredDecimalShape(row);

            // ODP.NET surfaces a NUMBER(p,s) with a small p as Double, which cannot hold what the
            // column declares; BINARY_DOUBLE reports no precision and stays a Double.
            if (clrType == typeof(double) && precision is not null)
                clrType = typeof(decimal);

            columns.Add(new PipeColumnInfo(name, clrType, allowNull,
                IsCaseSensitive: name != name.ToUpperInvariant(),
                Precision: clrType == typeof(decimal) ? precision : null,
                Scale: clrType == typeof(decimal) ? scale : null));
        }

        return columns;
    }

    // ODP.NET reports an unconstrained NUMBER (and FLOAT) as precision 38, scale 127: nothing declared.
    private const int UndeclaredScale = 127;

    private static (int? Precision, int? Scale) DeclaredDecimalShape(DataRow row)
    {
        if (row["NumericPrecision"] is not (short or int) || row["NumericScale"] is not (short or int))
            return (null, null);

        var precision = Convert.ToInt32(row["NumericPrecision"]);
        var scale = Convert.ToInt32(row["NumericScale"]);
        return scale == UndeclaredScale || precision < 1 ? (null, null) : (precision, scale);
    }

    /// <summary>Row mode reads the value ODP.NET converts to <see cref="decimal"/> where the column declares a width.</summary>
    protected override object? GetCellValue(int ordinal) =>
        Columns![ordinal].ClrType == typeof(decimal) && Reader!.GetFieldType(ordinal) == typeof(double)
            ? Reader.GetDecimal(ordinal)
            : base.GetCellValue(ordinal);

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
    }
}
