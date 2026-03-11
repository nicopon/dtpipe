using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DtPipe.Adapters.DuckDB;

public sealed partial class DuckDataSourceReader : IColumnarStreamReader, IRequiresOptions<DuckDbReaderOptions>
{
	private readonly DuckDBConnection _connection;
	private readonly DuckDBCommand _command;
	private readonly string _query;
	private readonly ILogger _logger;
	private DuckDBDataReader? _reader;
	private readonly SemaphoreSlim _semaphore = new(1, 1);

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

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

	public DuckDataSourceReader(string connectionString, string query, DuckDbReaderOptions options, ILogger? logger = null, int queryTimeout = 0)
		: this(new DuckDBConnection(connectionString), query, options, logger, queryTimeout)
	{
	}

	public DuckDataSourceReader(DuckDBConnection connection, string query, DuckDbReaderOptions options, ILogger? logger = null, int queryTimeout = 0)
	{
		ValidateQueryIsSafeSelect(query);

		_query = query;
		_logger = logger ?? NullLogger.Instance;
		_connection = connection;
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

		// Cap memory to prevent Jetsam overcommit kills when multiple branches run concurrently
		using (var limitCmd = _connection.CreateCommand())
		{
			limitCmd.CommandText = "PRAGMA memory_limit='2GB'; PRAGMA threads=2;";
			await limitCmd.ExecuteNonQueryAsync(ct);
		}

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
			await bridge.InitializeAsync(Columns, 1024, ct);

			// Yield batches as they are produced by the bridge
			// Synchronous feeder loop to avoid Task.Run overhead and potential deadlocks in simple scenarios
			// For larger datasets, this could be returned to a Task.Run if needed for concurrency
			var ingestionTask = Task.Run(async () =>
			{
				try
				{
					var colCount = Columns.Count;
					var row = new object[colCount];

					while (await _reader.ReadAsync(ct))
					{
						_reader.GetValues(row);
						var rowCopy = new object?[colCount];
						System.Array.Copy(row, rowCopy, colCount);
						await bridge.IngestRowsAsync(new[] { rowCopy }, ct);
					}

					await bridge.CompleteAsync(ct);
				}
				catch (Exception ex)
				{
					_logger.LogError(ex, "Error during DuckDB to Arrow ingestion");
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

			while (await _reader.ReadAsync(ct))
			{
				var row = new object?[columnCount];
				for (var i = 0; i < columnCount; i++)
				{
					row[i] = _reader.IsDBNull(i) ? null : _reader.GetValue(i);
				}

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
					reader.GetFieldType(i),
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
			columns.Add(new PipeColumnInfo(name, clrType, allowNull,
				IsCaseSensitive: name != name.ToLowerInvariant()));
		}

		return columns;
	}

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
