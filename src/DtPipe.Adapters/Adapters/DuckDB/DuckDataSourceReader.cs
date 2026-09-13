using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Apache.Arrow;
using DtPipe.Adapters.Shared.Infrastructure.DuckDb;
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

/// <summary>
/// Reads a DuckDB query through the Arrow C Data interface.
/// </summary>
/// <remarks>
/// DuckDB is columnar, so a row reader made the data cross into rows and back into columns to
/// reach Arrow — a detour that boxed every cell and lost any type the driver's schema table could
/// not name, which is why a STRUCT or a MAP used to arrive as text. The interface takes the
/// columns as they already are, and it is the same one the SQL processor reads through, so the
/// two paths cannot report different values for the same column again.
/// </remarks>
public sealed partial class DuckDataSourceReader : IColumnarStreamReader, IRequiresOptions<DuckDbReaderOptions>, IBatchSizeConfigurable
{
	private readonly DuckDBConnection _connection;
	private readonly bool _ownsConnection;
	private readonly DuckHubConnectionInfo _hubInfo;
	private readonly string _query;
	private readonly string? _initSql;
	private readonly IStringContentResolver? _resolver;
	private readonly ILogger _logger;
	private readonly IMcpSecurityContext? _mcpSecurityContext;
	private readonly SemaphoreSlim _semaphore = new(1, 1);
	private DuckDbArrowResultReader? _arrowReader;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
	public Schema? Schema { get; private set; }

	// Accepted for interface parity but not enforced: DuckDB hands out its own fixed-size Arrow
	// chunks, and the reader passes them on rather than regrouping them. The byte cap matters on
	// the wire-decode readers (ADO, Postgres COPY).
	public int BatchSize { get; set; } = PipelineOptions.DefaultBatchSize;
	public long MaxBatchBytes { get; set; }

	[GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
	private static partial Regex FirstWordRegex();

	// queryTimeout is accepted for call-site parity and ignored: it only ever reached
	// DuckDBCommand.CommandTimeout, which DuckDB.NET declares to satisfy the ADO contract and
	// never reads. Cancellation is the token.
	public DuckDataSourceReader(string connectionString, string query, DuckDbReaderOptions options, ILogger? logger = null, int queryTimeout = 0, IStringContentResolver? resolver = null, IMcpSecurityContext? mcpSecurityContext = null)
	{
		_hubInfo = DuckHubConnectionParser.Parse(options.Variant, connectionString);
		_connection = new DuckDBConnection(_hubInfo.EffectiveConnectionString);
		_ownsConnection = true;

		ValidateQueryIsSafeSelect(query);

		_query = query;
		_initSql = options.InitSql;
		_resolver = resolver;
		_logger = logger ?? NullLogger.Instance;
		_mcpSecurityContext = mcpSecurityContext;
	}

	// Same contract, over a connection the caller owns and keeps.
	public DuckDataSourceReader(DuckDBConnection connection, string query, DuckDbReaderOptions options, ILogger? logger = null, int queryTimeout = 0, IStringContentResolver? resolver = null, IMcpSecurityContext? mcpSecurityContext = null)
	{
		_hubInfo = new DuckHubConnectionInfo { IsHub = false };
		ValidateQueryIsSafeSelect(query);

		_query = query;
		_initSql = options.InitSql;
		_resolver = resolver;
		_logger = logger ?? NullLogger.Instance;
		_connection = connection;
		_ownsConnection = false;
		_mcpSecurityContext = mcpSecurityContext;
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
	}

	public async Task OpenAsync(CancellationToken ct = default)
	{
		if (_connection.State != System.Data.ConnectionState.Open)
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
		await DuckDbArrowResultReader.ApplyArrowSessionSettingsAsync(_connection, ct);

		_arrowReader = new DuckDbArrowResultReader(_connection, _query, _logger);
		Schema = _arrowReader.Prepare();
		Columns = Schema.FieldsList
			.Select(f => new PipeColumnInfo(
				f.Name,
				ArrowTypeMapper.GetClrTypeFromField(f),
				f.IsNullable,
				// Not case-sensitive, like MySQL, SQLite and SQL Server and unlike PostgreSQL and
				// Oracle. The rule those two use — a stored name that differs from the engine's
				// folding must have been quoted when it was created — needs the engine to fold,
				// and DuckDB does not: a bare CREATE TABLE t(MyCol INT) stores "MyCol" and
				// resolves it case-insensitively.
				IsCaseSensitive: false))
			.ToList();
	}

	public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync([EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_arrowReader is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		await _semaphore.WaitAsync(ct);
		try
		{
			await foreach (var batch in _arrowReader.ReadRecordBatchesAsync(ct))
				yield return batch;
		}
		finally
		{
			_semaphore.Release();
		}
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[EnumeratorCancellation] CancellationToken ct = default)
	{
		await foreach (var batch in ReadRecordBatchesAsync(ct))
		{
			using (batch)
				yield return ConvertBatchToRows(batch);
		}
	}

	private static ReadOnlyMemory<object?[]> ConvertBatchToRows(RecordBatch batch)
	{
		var rows = new object?[batch.Length][];
		for (var r = 0; r < batch.Length; r++)
		{
			rows[r] = new object?[batch.ColumnCount];
			for (var c = 0; c < batch.ColumnCount; c++)
			{
				var column = batch.Column(c);
				rows[r][c] = column is null
					? null
					: ArrowTypeMapper.GetValueForField(column, batch.Schema.GetFieldByIndex(c), r);
			}
		}
		return rows;
	}

	public async ValueTask DisposeAsync()
	{
		await _semaphore.WaitAsync();
		try
		{
			_arrowReader?.Dispose();
			_arrowReader = null;
			if (_ownsConnection)
				await _connection.DisposeAsync();
		}
		finally
		{
			_semaphore.Release();
			_semaphore.Dispose();
		}
	}
}
