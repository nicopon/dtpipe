using System.Data;
using Apache.Arrow.Ado;
using Apache.Arrow.Types;
using DtPipe.Adapters.Common;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using MySqlConnector;

namespace DtPipe.Adapters.MySql;

/// <summary>
/// Columnar stream reader for MySQL. Produces Apache Arrow RecordBatches from MySqlDataReader
/// through typed column consumers.
/// Implements both IStreamReader (row-mode fallback) and IColumnarStreamReader (Arrow mode).
/// </summary>
public sealed class MySqlReader : AdoColumnarReader
{
	private Func<IArrowType, int, IAdoConsumer>? _consumerFactory;

	public MySqlReader(string connectionString, string query, int queryTimeout = 0)
	{
		ValidateQueryIsSafeSelect(query, "SHOW", "DESCRIBE", "EXPLAIN");
		Connection = new MySqlConnection(connectionString);
		Command = new MySqlCommand(query, (MySqlConnection)Connection) { CommandTimeout = queryTimeout };
	}

	public override async Task OpenAsync(CancellationToken ct = default)
	{
		await Connection!.OpenAsync(ct);

		// No explicit transaction here, unlike the SQL Server reader. MySQL's default InnoDB
		// isolation is REPEATABLE READ, which already gives a consistent snapshot for the life of
		// a transaction — but opening one would hold it across the whole stream, pinning undo log
		// on the server for a read that may last minutes. Streaming outside a transaction matches
		// what mysqldump --single-transaction avoids paying when consistency is not requested.
		Reader = await Command!.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);

		var dbColumns = ((MySqlDataReader)Reader).GetColumnSchema();

		// Build PipeColumnInfo from DB schema (CLR types) — authoritative for DtPipe pipeline
		var columns = new List<PipeColumnInfo>(dbColumns.Count);
		foreach (var col in dbColumns)
		{
			columns.Add(new PipeColumnInfo(
				col.ColumnName,
				col.DataType ?? typeof(object),
				col.AllowDBNull ?? true,
				// MySQL compares column identifiers case-insensitively regardless of the server's
				// lower_case_table_names, so no column ever needs case-preserving quoting.
				IsCaseSensitive: false,
				Precision: (int?)col.NumericPrecision,
				Scale: (int?)col.NumericScale
			));
		}
		Columns = columns;

		// Build Arrow schema from PipeColumnInfo via ArrowTypeMapper — guarantees consistency
		Schema = ArrowSchemaFactory.Create(Columns);

		// MySQL has no UUID type; MySqlConnector surfaces CHAR(36) columns as Guid under its
		// default GuidFormat. Route those through the same consumer SQL Server uses so the value
		// lands as canonical arrow.uuid bytes rather than a 36-character string.
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
}
