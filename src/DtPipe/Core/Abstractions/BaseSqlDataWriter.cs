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

	public abstract ISqlDialect Dialect { get; }

	protected BaseSqlDataWriter(string connectionString)
	{
		_connectionString = connectionString;
	}

	#region ISchemaInspector Implementation
	private TargetSchemaInfo? _cachedSchema;

	/// <summary>
	/// Implementation of ISchemaInspector. Returns a cached result if available.
	/// Derived classes should implement InspectTargetInternalAsync.
	/// </summary>
	public virtual async Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default)
	{
		if (_cachedSchema != null) return _cachedSchema;
		_cachedSchema = await InspectTargetInternalAsync(ct);
		return _cachedSchema;
	}

	/// <summary>
	/// Actual implementation of schema inspection, to be implemented by derived classes.
	/// </summary>
	protected abstract Task<TargetSchemaInfo?> InspectTargetInternalAsync(CancellationToken ct);
	#endregion

	#region IKeyValidator Implementation (Virtual with default empty)
	public virtual string? GetWriteStrategy() => "Unknown";
	public virtual IReadOnlyList<string>? GetRequestedPrimaryKeys() => null;
	public virtual bool RequiresPrimaryKey() => false;
	#endregion

	public async ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
	{
		_columns = NormalizeColumns(columns);

		await EnsureConnectionOpenAsync(ct);

		var (resolvedSchema, resolvedTable) = await ResolveTargetTableAsync(ct);
		_quotedTargetTableName = BuildQuotedTableName(resolvedSchema, resolvedTable);

		await ApplyWriteStrategyAsync(resolvedSchema, resolvedTable, ct);

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
				// Normalize to dialect default (e.g. lowercase for PG) to match target schema expectations
				normalized.Add(col with { Name = Dialect.Normalize(col.Name) });
			}
		}
		return normalized;
	}

	/// <summary>
	/// Synchronizes source column metadata with the target schema information.
	/// Updates Name (to match exact casing in target DB), IsCaseSensitive flag,
	/// and ClrType (to enable correct type conversion, e.g. string → DateTime).
	/// Columns not found in the target schema are kept unchanged.
	/// </summary>
	protected void SyncColumnsFromIntrospection(TargetSchemaInfo targetSchema)
	{
		if (_columns == null) return;

		var synced = new List<PipeColumnInfo>(_columns.Count);
		foreach (var col in _columns)
		{
			var targetCol = targetSchema.Columns.FirstOrDefault(
				tc => tc.Name.Equals(col.Name, StringComparison.OrdinalIgnoreCase)
			);

			if (targetCol != null)
			{
				synced.Add(col with
				{
					Name = targetCol.Name,
					IsCaseSensitive = targetCol.IsCaseSensitive,
					ClrType = targetCol.InferredClrType ?? col.ClrType
				});
			}
			else
			{
				synced.Add(col);
			}
		}
		_columns = synced;
	}

	protected virtual async Task EnsureConnectionOpenAsync(CancellationToken ct)
	{
		if (_connection == null)
		{
			_connection = CreateConnection(_connectionString);
		}
		if (_connection.State != ConnectionState.Open)
		{
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
