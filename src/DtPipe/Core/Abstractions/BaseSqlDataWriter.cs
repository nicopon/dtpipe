using System.Data;
using DtPipe.Core.Models;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Abstract base class for SQL-based DataWriters that share common lifecycle logic:
/// Connection management, Table resolution, Strategy handling (Recreate/Truncate/etc.), and Schema introspection.
/// </summary>
public abstract class BaseSqlDataWriter : IDataWriter, ISchemaInspector, IKeyValidator
{
	protected readonly string _connectionString;
	protected IDbConnection? _connection;
	protected string _quotedTargetTableName = "";
	protected IReadOnlyList<PipeColumnInfo>? _columns;

	// Derived classes must provide the dialect for quoting and normalization
	public abstract ISqlDialect Dialect { get; }

	protected BaseSqlDataWriter(string connectionString)
	{
		_connectionString = connectionString;
	}

	#region ISchemaInspector Implementation (Abstract or Virtual)
	public abstract Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default);
	#endregion

	#region IKeyValidator Implementation (Virtual with default empty)
	public virtual string? GetWriteStrategy() => "Unknown";
	public virtual IReadOnlyList<string>? GetRequestedPrimaryKeys() => null;
	public virtual bool RequiresPrimaryKey() => false;
	#endregion

	/// <summary>
	/// Initializes the writer by normalizing columns, opening the connection, resolving the target table, and applying the write strategy.
	/// This ensures the writer is ready to accept batches of data.
	/// </summary>
	public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		// 1. Normalize Column Names based on dialect rules (e.g. lowercase for Postgres)
		_columns = NormalizeColumns(columns);

		// 2. Open Connection (ensuring async opening where possible)
		await EnsureConnectionOpenAsync(ct);

		// 3. Resolve Target Table (handling synonyms, default schemas, etc.)
		var (resolvedSchema, resolvedTable) = await ResolveTargetTableAsync(ct);

		// Compute quoted name once for efficiency
		_quotedTargetTableName = BuildQuotedTableName(resolvedSchema, resolvedTable);

		// 4. Apply Write Strategy (Recreate, Truncate, Append, etc.)
		await ApplyWriteStrategyAsync(resolvedSchema, resolvedTable, ct);

		// 5. Post-Initialization Hook (e.g. prepare commands, configure bulk copy)
		await OnInitializedAsync(ct);
	}

	protected virtual List<PipeColumnInfo> NormalizeColumns(IReadOnlyList<PipeColumnInfo> columns)
	{
		var normalized = new List<PipeColumnInfo>(columns.Count);
		foreach (var col in columns)
		{
			if (col.IsCaseSensitive)
			{
				normalized.Add(col);
			}
			else
			{
				// Normalize to dialect default (e.g. lowercase for PG, uppercase for Oracle)
				// This prevents issues where source column "ID" becomes "id" in PG but "ID" was expected
				normalized.Add(col with { Name = Dialect.Normalize(col.Name) });
			}
		}
		return normalized;
	}

	protected virtual async Task EnsureConnectionOpenAsync(CancellationToken ct)
	{
		if (_connection == null)
		{
			_connection = CreateConnection(_connectionString);
		}
		if (_connection.State != ConnectionState.Open)
		{
			// IDbConnection.Open() is synchronous, but we wrap it in Task.Run if needed or just call it.
			// Async methods are usually on DbConnection (System.Data.Common).
			// Here we assume the derived class creates a DbConnection which has OpenAsync.
			// Casting to DbConnection to access OpenAsync if possible.
			if (_connection is System.Data.Common.DbConnection dbConn)
			{
				await dbConn.OpenAsync(ct);
			}
			else
			{
				_connection.Open();
			}
		}
	}

	/// <summary>
	/// Derived classes must implement connection creation.
	/// </summary>
	protected abstract IDbConnection CreateConnection(string connectionString);

	/// <summary>
	/// Resolves the effective Schema and Table name. 
	/// If table doesn't exist, returns the parsed strategy intention.
	/// </summary>
	protected abstract Task<(string Schema, string Table)> ResolveTargetTableAsync(CancellationToken ct);

	protected virtual string BuildQuotedTableName(string schema, string table)
	{
		var safeSchema = Dialect.NeedsQuoting(schema) ? Dialect.Quote(schema) : schema;
		var safeTable = Dialect.NeedsQuoting(table) ? Dialect.Quote(table) : table;
		return string.IsNullOrEmpty(safeSchema) ? safeTable : $"{safeSchema}.{safeTable}";
	}

	protected abstract Task ApplyWriteStrategyAsync(string resolvedSchema, string resolvedTable, CancellationToken ct);

	/// <summary>
	/// Hook for post-initialization logic (e.g. creating specific commands, bulk copy instances).
	/// </summary>
	protected virtual ValueTask OnInitializedAsync(CancellationToken ct) => ValueTask.CompletedTask;

	public abstract ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default);

	public abstract ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default);

	public virtual async ValueTask CompleteAsync(CancellationToken ct = default)
	{
		// Default no-op
		await ValueTask.CompletedTask;
	}

	public async ValueTask DisposeAsync()
	{
		await DisposeResourcesAsync();

		if (_connection != null)
		{
			if (_connection is IAsyncDisposable asyncDisposable)
			{
				await asyncDisposable.DisposeAsync();
			}
			else
			{
				_connection.Dispose();
			}
			_connection = null;
		}
	}

	protected abstract ValueTask DisposeResourcesAsync();

	// Abstract SQL Generators
	protected abstract string GetCreateTableSql(string tableName, IEnumerable<PipeColumnInfo> columns);
	protected abstract string GetTruncateTableSql(string tableName);
	protected abstract string GetDropTableSql(string tableName);

	// Helpers for Strategy Implementation

	protected async Task ExecuteNonQueryAsync(string sql, CancellationToken ct)
	{
		if (_connection == null) throw new InvalidOperationException("Connection not initialized");
		using var cmd = _connection.CreateCommand();
		cmd.CommandText = sql;
		if (cmd is System.Data.Common.DbCommand dbCmd)
		{
			await dbCmd.ExecuteNonQueryAsync(ct);
		}
		else
		{
			cmd.ExecuteNonQuery();
		}
	}
}
