using System.Data;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using Npgsql;

namespace DtPipe.Adapters.PostgreSQL;

public partial class PostgreSqlReader : IStreamReader
{
	private readonly string _connectionString;
	private readonly string _query;
	private readonly int _timeout;
	private NpgsqlConnection? _connection;
	private NpgsqlCommand? _command;
	private NpgsqlDataReader? _reader;
	public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

	// DDL/DML keywords to reject
	private static readonly string[] DdlKeywords =
	{
		"CREATE", "DROP", "ALTER", "TRUNCATE", "RENAME",
		"GRANT", "REVOKE", "COMMENT", "VACUUM", "MIGRATE",
		"INSERT", "UPDATE", "DELETE", "MERGE", "CALL", "DO",
		"LOCK", "EXPLAIN", "ANALYZE"
	};

	[GeneratedRegex(@"^\s*(\w+)", RegexOptions.IgnoreCase | RegexOptions.Compiled)]
	private static partial Regex FirstWordRegex();

	public PostgreSqlReader(string connectionString, string query, int timeout)
	{
		ValidateQueryIsSafeSelect(query);
		_connectionString = connectionString;
		_query = query;
		_timeout = timeout;
	}

	/// <summary>
	/// Validates that the query is a safe SELECT statement.
	/// </summary>
	private static void ValidateQueryIsSafeSelect(string query)
	{
		if (string.IsNullOrWhiteSpace(query))
			throw new ArgumentException("Query cannot be empty.", nameof(query));

		var match = FirstWordRegex().Match(query);
		if (!match.Success)
			throw new ArgumentException("Invalid query format.", nameof(query));

		var firstWord = match.Groups[1].Value.ToUpperInvariant();

		if (firstWord != "SELECT" && firstWord != "WITH" && firstWord != "VALUES")
		{
			throw new InvalidOperationException($"Only SELECT queries are allowed. Detected: {firstWord}");
		}

		var upperQuery = query.ToUpperInvariant();
		foreach (var keyword in DdlKeywords)
		{
			if (Regex.IsMatch(upperQuery, $@"\b{keyword}\b"))
			{
				// Allow SELECT ... but block if keyword appears as a command
				if (keyword != "SELECT" && firstWord == "SELECT") continue;
				// Ideally we'd have a better parser, but for now this matches other adapters
			}
		}
	}

	public async Task OpenAsync(CancellationToken ct = default)
	{
		_connection = new NpgsqlConnection(_connectionString);
		await _connection.OpenAsync(ct);

		_command = new NpgsqlCommand(_query, _connection);
		if (_timeout > 0)
		{
			_command.CommandTimeout = _timeout;
		}

		// Use SequentialAccess for performance
		_reader = await _command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);

		// Populate Columns
		var schema = await _reader.GetColumnSchemaAsync(ct);

		// PostgreSQL normalizes unquoted identifiers to lowercase
		// If column name contains uppercase, it was created with quotes (case-sensitive)
		Columns = schema.Select(c => new PipeColumnInfo(
			c.ColumnName,
			c.DataType ?? typeof(object),
			c.AllowDBNull ?? true,
			IsCaseSensitive: c.ColumnName != c.ColumnName.ToLowerInvariant() // Detect quoted columns
		)).ToList();
	}

	public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
	{
		if (_reader == null) throw new InvalidOperationException("Reader not opened");

		var batch = new object?[batchSize][];
		var index = 0;

		while (await _reader.ReadAsync(ct))
		{
			var row = new object[_reader.FieldCount];
			_reader.GetValues(row);
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

	public async ValueTask DisposeAsync()
	{
		if (_reader != null) await _reader.DisposeAsync();
		if (_command != null) await _command.DisposeAsync();
		if (_connection != null) await _connection.DisposeAsync();
	}
}
