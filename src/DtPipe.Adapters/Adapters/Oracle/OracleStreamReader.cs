using System.Data;
using System.Text.RegularExpressions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Oracle.ManagedDataAccess.Client;

namespace DtPipe.Adapters.Oracle;

public sealed partial class OracleStreamReader : IStreamReader, IRequiresOptions<OracleReaderOptions>
{
	private readonly OracleConnection _connection;
	private readonly OracleCommand _command;
	private readonly string _query;
	private OracleDataReader? _reader;

	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

	// DDL/DML keywords that should be rejected to prevent modification of data or schema
	private static readonly string[] DdlKeywords =
	{
		"CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
		"GRANT", "REVOKE", "COMMENT", "FLASHBACK", "PURGE",
		"INSERT", "UPDATE", "DELETE", "MERGE", "CALL",
		"LOCK", "EXECUTE", "EXPLAIN"
	};

	[GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
	private static partial Regex FirstWordRegex();

	public OracleStreamReader(string connectionString, string query, OracleReaderOptions options, int queryTimeout = 0)
	{
		ValidateQueryIsSafeSelect(query);

		_query = query;
		_connection = new OracleConnection(connectionString);
		_command = new OracleCommand(query, _connection)
		{
			FetchSize = options.FetchSize,
			CommandTimeout = queryTimeout
		};
	}

	/// <summary>
	/// Validates that the query is a safe SELECT statement.
	/// Throws if DDL/DML statements are detected to prevent SQL injection or accidental modification.
	/// </summary>
	private static void ValidateQueryIsSafeSelect(string query)
	{
		if (string.IsNullOrWhiteSpace(query))
			throw new ArgumentException("Query cannot be empty.", nameof(query));

		var match = FirstWordRegex().Match(query);
		if (!match.Success)
			throw new ArgumentException("Invalid query format.", nameof(query));

		var firstWord = match.Groups[1].Value.ToUpperInvariant();

		if (firstWord != "SELECT" && firstWord != "WITH")
		{
			throw new InvalidOperationException(
				$"Only SELECT queries are allowed. Detected: {firstWord}. " +
				"DDL/DML statements (CREATE, DROP, INSERT, UPDATE, DELETE, etc.) are blocked for safety.");
		}

		var upperQuery = query.ToUpperInvariant();
		foreach (var keyword in DdlKeywords)
		{
			if (Regex.IsMatch(upperQuery, $@"\b{keyword}\b"))
			{
				if (keyword != "SELECT" && firstWord == "SELECT")
				{
					continue;
				}
			}
		}
	}

	public async Task OpenAsync(CancellationToken ct = default)
	{
		await _connection.OpenAsync(ct);
		_reader = (OracleDataReader)await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
		Columns = ExtractColumns(_reader);
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
		int batchSize,
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader is null)
			throw new InvalidOperationException("Call OpenAsync first.");

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

	public async IAsyncEnumerable<object?[]> ReadRowsAsync(
		[System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader is null)
			throw new InvalidOperationException("Call OpenAsync first.");

		var columnCount = _reader.FieldCount;

		while (await _reader.ReadAsync(ct))
		{
			var row = new object?[columnCount];
			for (var i = 0; i < columnCount; i++)
			{
				row[i] = _reader.IsDBNull(i) ? null : _reader.GetValue(i);
			}
			yield return row;
		}
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
				columns.Add(new PipeColumnInfo(
					name,
					reader.GetFieldType(i),
					true,
					IsCaseSensitive: name != name.ToUpperInvariant()
				));
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

	public async ValueTask DisposeAsync()
	{
		if (_reader is not null)
		{
			await _reader.DisposeAsync();
		}
		await _command.DisposeAsync();
		await _connection.DisposeAsync();
	}
}
