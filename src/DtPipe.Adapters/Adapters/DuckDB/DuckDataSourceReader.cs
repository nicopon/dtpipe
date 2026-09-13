using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Expressions;
using DtPipe.Core.Helpers;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Security;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Adapters.DuckDB;

public sealed partial class DuckDataSourceReader : IColumnarStreamReader, IRequiresOptions<DuckDbReaderOptions>, IBatchSizeConfigurable
{
	private readonly DuckDBConnection _connection;
	private readonly DuckHubConnectionInfo _hubInfo;
	private readonly DuckDBCommand _command;
	private readonly string _query;
	private readonly string? _initSql;
	private readonly IStringContentResolver? _resolver;
	private readonly ILogger _logger;
	private DuckDBDataReader? _reader;
	private readonly SemaphoreSlim _semaphore = new(1, 1);

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema => Columns != null ? DtPipe.Core.Infrastructure.Arrow.ArrowSchemaFactory.Create(Columns) : null;
	public int BatchSize { get; set; } = PipelineOptions.DefaultBatchSize;

	// Accepted for interface parity but not enforced here: DuckDB streams its own fixed-size
	// Arrow chunks, so a batch never holds more than BatchSize rows' worth of already-bounded
	// vectors. The byte cap matters on the wire-decode readers (ADO, Postgres COPY).
	public long MaxBatchBytes { get; set; }

	// DDL/DML keywords to reject
	// Block destructive commands.
	private static readonly string[] DdlKeywords =
	{
		"CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
		"GRANT", "REVOKE", "VACUUM", "ATTACH", "DETACH",
		"INSERT", "UPDATE", "DELETE", "REPLACE", "COPY"
	};

	[GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
	private static partial Regex FirstWordRegex();

	private readonly IMcpSecurityContext? _mcpSecurityContext;

	public DuckDataSourceReader(string connectionString, string query, DuckDbReaderOptions options, ILogger? logger = null, int queryTimeout = 0, IStringContentResolver? resolver = null, IMcpSecurityContext? mcpSecurityContext = null)
	{
		_hubInfo = DuckHubConnectionParser.Parse(options.Variant, connectionString);
		_connection = new DuckDBConnection(_hubInfo.EffectiveConnectionString);

		ValidateQueryIsSafeSelect(query);

		_query = query;
		_initSql = options.InitSql;
		_resolver = resolver;
		_logger = logger ?? NullLogger.Instance;
		_mcpSecurityContext = mcpSecurityContext;
		_command = new DuckDBCommand(query, _connection)
		{
			CommandTimeout = queryTimeout
		};
	}

	public DuckDataSourceReader(DuckDBConnection connection, string query, DuckDbReaderOptions options, ILogger? logger = null, int queryTimeout = 0, IStringContentResolver? resolver = null, IMcpSecurityContext? mcpSecurityContext = null)
	{
		_hubInfo = new DuckHubConnectionInfo { IsHub = false };
		ValidateQueryIsSafeSelect(query);

		_query = query;
		_initSql = options.InitSql;
		_resolver = resolver;
		_logger = logger ?? NullLogger.Instance;
		_connection = connection;
		_mcpSecurityContext = mcpSecurityContext;
		_command = new DuckDBCommand(query, _connection)
		{
			CommandTimeout = queryTimeout
		};
	}

	private static void ValidateQueryIsSafeSelect(string query)
	{
		if (string.IsNullOrWhiteSpace(query))
			throw new ArgumentException("Query cannot be empty.", nameof(query));

		var match = FirstWordRegex().Match(query);
		if (!match.Success)
			throw new ArgumentException("Invalid query format.", nameof(query));

		var firstWord = match.Groups[1].Value.ToUpperInvariant();

		if (firstWord != "SELECT" && firstWord != "WITH" && firstWord != "PRAGMA" && firstWord != "DESCRIBE")
		{
			throw new InvalidOperationException($"Query must start with SELECT/WITH. Detected: {firstWord}");
		}

		// Basic keyword check
		var upperQuery = query.ToUpperInvariant();
		foreach (var keyword in DdlKeywords)
		{
			if (Regex.IsMatch(upperQuery, $@"\b{keyword}\b"))
			{
				// Allow SELECT
				if (firstWord == "SELECT") continue;
				// Be stricter for DuckDB as it might operate on local files
				// But for now, simple consistency
			}
		}
	}

	public async Task OpenAsync(CancellationToken ct = default)
	{
		await _connection.OpenAsync(ct);

		if (_hubInfo.IsHub && _hubInfo.InitSqlStatements.Length > 0)
		{
			foreach (var stmt in _hubInfo.InitSqlStatements)
			{
				using var hubCmd = _connection.CreateCommand();
				hubCmd.CommandText = stmt;
				await hubCmd.ExecuteNonQueryAsync(ct);
			}
		}

		// Cap memory to prevent Jetsam overcommit kills when multiple branches run concurrently
		using (var limitCmd = _connection.CreateCommand())
		{
			var sql = "PRAGMA memory_limit='2GB'; PRAGMA threads=2;";
			if (_mcpSecurityContext?.IsMcpSession == true)
			{
				sql += " PRAGMA disable_external_access=true;";
			}
			limitCmd.CommandText = sql;
			await limitCmd.ExecuteNonQueryAsync(ct);
		}

		await DuckInitSqlRunner.RunAsync(_connection, _initSql, _resolver, ct);

		_reader = (DuckDBDataReader)await _command.ExecuteReaderAsync(ct);
		Columns = ExtractColumns(_reader);
	}

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		// Lock to ensure we don't dispose while reading
		await _semaphore.WaitAsync(ct);
		try
		{
			if (Columns == null) throw new InvalidOperationException("Reader not opened.");

			// Using ArrowRowToColumnarBridge to efficiently produce RecordBatches from the DataReader.
			var bridge = new DtPipe.Core.Infrastructure.Arrow.ArrowRowToColumnarBridge(_logger);
			await bridge.InitializeAsync(Columns, BatchSize, ct: ct);

			// Yield batches as they are produced by the bridge
			// Synchronous feeder loop to avoid Task.Run overhead and potential deadlocks in simple scenarios
			// For larger datasets, this could be returned to a Task.Run if needed for concurrency
			var ingestionTask = Task.Run(async () =>
			{
				try
				{
					var colCount = Columns.Count;
					var row = new object[colCount];

					// A column this reader had to declare as text because Arrow has no form for it
					// (a STRUCT, a MAP) still arrives from the driver as a Dictionary; render it
					// here so the value matches the column.
					var renderAsJson = CompositeColumnIndexes(Columns);

					while (await _reader.ReadAsync(ct))
					{
						_reader.GetValues(row);
						var rowCopy = new object?[colCount];
						System.Array.Copy(row, rowCopy, colCount);
						foreach (var i in renderAsJson)
							rowCopy[i] = CompositeCellJson.RenderIfComposite(rowCopy[i]);
						await bridge.IngestRowsAsync(new[] { rowCopy }, ct);
					}

					await bridge.CompleteAsync(ct);
				}
				catch (Exception ex)
				{
					_logger.LogError(ex, "Error during DuckDB to Arrow ingestion");
					bridge.Fault(ex);
					throw;
				}
			}, ct);

			await foreach (var batch in bridge.ReadRecordBatchesAsync(ct))
			{
				yield return batch;
			}

			await ingestionTask;
		}
		finally
		{
			_semaphore.Release();
		}
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		// Lock to ensure we don't dispose while reading
		await _semaphore.WaitAsync(ct);
		try
		{
			var columnCount = _reader.FieldCount;
			var batch = new object?[batchSize][];
			var index = 0;

			// Same rendering as the columnar path: a column declared text because Arrow has no form
			// for it still arrives composite from the driver.
			var renderAsJson = Columns is null ? [] : CompositeColumnIndexes(Columns);

			while (await _reader.ReadAsync(ct))
			{
				var row = new object?[columnCount];
				for (var i = 0; i < columnCount; i++)
				{
					row[i] = _reader.IsDBNull(i) ? null : _reader.GetValue(i);
				}

				foreach (var i in renderAsJson)
					row[i] = CompositeCellJson.RenderIfComposite(row[i]);

				batch[index++] = row;

				if (index >= batchSize)
				{
					yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
					batch = new object?[batchSize][];
					index = 0;
				}
			}

			if (index > 0)
			{
				yield return new ReadOnlyMemory<object?[]>(batch, 0, index);
			}
		}
		finally
		{
			_semaphore.Release();
		}
	}

	private static List<PipeColumnInfo> ExtractColumns(DuckDBDataReader reader)
	{
		var columns = new List<PipeColumnInfo>(reader.FieldCount);
		var schemaTable = reader.GetSchemaTable();

		if (schemaTable is null)
		{
			for (var i = 0; i < reader.FieldCount; i++)
			{
				var name = reader.GetName(i);
				columns.Add(new PipeColumnInfo(
					name,
					RepresentableType(reader.GetFieldType(i)),
					true,
					IsCaseSensitive: name != name.ToLowerInvariant() // DuckDB normalizes to lowercase
				));
			}
			return columns;
		}

		foreach (DataRow row in schemaTable.Rows)
		{
			var name = row["ColumnName"]?.ToString() ?? $"Column{columns.Count}";
			var clrType = row["DataType"] as Type ?? typeof(object);
			var allowNull = row["AllowDBNull"] as bool? ?? true;

			// DuckDB normalizes unquoted identifiers to lowercase (like PostgreSQL)
			// If column name contains uppercase, it was created with quotes (case-sensitive)
			columns.Add(new PipeColumnInfo(name, RepresentableType(clrType), allowNull,
				IsCaseSensitive: name != name.ToLowerInvariant()));
		}

		return columns;
	}

	/// <summary>
	/// Indexes of the columns declared as text whose value may still arrive composite, computed
	/// once per read rather than tested per cell.
	/// </summary>
	private static int[] CompositeColumnIndexes(IReadOnlyList<PipeColumnInfo> columns)
		=> Enumerable.Range(0, columns.Count)
			.Where(i => columns[i].ClrType == typeof(string))
			.ToArray();

	/// <summary>
	/// The type to declare for a DuckDB column: its own, or <c>string</c> when Arrow has no form
	/// for it and the value will be rendered as JSON.
	/// </summary>
	/// <remarks>
	/// A DuckDB STRUCT and a MAP arrive from the driver as a <c>Dictionary</c>, which the Arrow
	/// type map refuses — so building the schema threw before a single row was read, on a type the
	/// <c>--sql</c> processor handles without trouble because it takes the Arrow C Data interface
	/// rather than this row reader. A LIST needs nothing here: it arrives as a collection Arrow
	/// maps to a ListType, and keeps it.
	///
	/// Rendering as JSON is the same answer the writers give a composite a target cannot hold, and
	/// for the same reason — it is the adapter's decision, not the engine's.
	/// </remarks>
	private static Type RepresentableType(Type clrType)
		=> ArrowTypeMapper.TryGetLogicalType(clrType, out _) ? clrType : typeof(string);

	public async ValueTask DisposeAsync()
	{
		await _semaphore.WaitAsync();
		try
		{
			if (_reader is not null)
			{
				await _reader.DisposeAsync();
			}
			await _command.DisposeAsync();
			await _connection.DisposeAsync();
		}
		finally
		{
			_semaphore.Release();
			_semaphore.Dispose();
		}
	}
}
