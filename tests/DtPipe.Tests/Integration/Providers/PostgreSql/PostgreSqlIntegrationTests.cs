using DtPipe.Adapters.PostgreSQL;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace DtPipe.Tests;

[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class PostgreSqlIntegrationTests : IAsyncLifetime
{
	private PostgreSqlContainer? _postgres;
	private string? _connectionString;

	public async ValueTask InitializeAsync()
	{
		_connectionString = await DockerHelper.GetPostgreSqlConnectionString(async () =>
		{
			_postgres = new PostgreSqlBuilder("postgres:15-alpine").Build();
			await _postgres.StartAsync();
			return _postgres.GetConnectionString();
		});

		if (_connectionString == null) return;

		await using var connection = new NpgsqlConnection(_connectionString);
		await connection.OpenAsync();

		await using var cmd = connection.CreateCommand();
		cmd.CommandText = "DROP TABLE IF EXISTS test_data; " + TestDataSeeder.GenerateTableDDL(connection, "test_data");
		await cmd.ExecuteNonQueryAsync();

		await TestDataSeeder.SeedAsync(connection, "test_data");
	}

	public async ValueTask DisposeAsync()
	{
		if (_postgres is not null)
		{
			await _postgres.DisposeAsync();
		}
	}

	[Fact]
	public async Task PostgreSqlReader_ReadsAllRows()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;

		await using var reader = new PostgreSqlReader(
			connectionString,
			"SELECT * FROM test_data ORDER BY Id", // Unquoted identifier for case-insensitive match (Postgres defaults to lowercase)
			timeout: 30);

		await reader.OpenAsync();

		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(10))
		{
			for (int i = 0; i < batch.Length; i++)
			{
				rows.Add(batch.Span[i]);
			}
		}

		Assert.Equal(4, rows.Count);
		Assert.Equal(7, reader.Columns!.Count);
		// Postgres returns lower case column names usually unless quoted in creation?
		// TestDataSeeder uses unquoted CREATE TABLE names, so Postgres lowercases them.
		Assert.Equal("id", reader.Columns[0].Name.ToLower());

		var alice = rows.First(r => r[0]?.ToString() == "1");
		// Check boolean
		Assert.Equal(true, alice[3]);
		// Check numeric
		Assert.Equal(95.50m, alice[4]);
	}

	[Fact]
	public async Task PostgreSqlWriter_WritesRows()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var targetTable = "test_export";

		// Options
		var options = new PostgreSqlWriterOptions
		{
			Table = targetTable,
			Strategy = PostgreSqlWriteStrategy.Truncate
		};

		// Prepare Data
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
		{
			new("id", typeof(int), false),
			new("name", typeof(string), true),
			new("created", typeof(DateTime), true)
		};

		var data = new List<object?[]>
		{
			new object?[] { 1, "Test 1", DateTime.UtcNow },
			new object?[] { 2, "Test 2", DateTime.UtcNow.AddDays(-1) }
		};

		// Act
		await using var writer = new PostgreSqlDataWriter(connectionString, options, NullLogger<PostgreSqlDataWriter>.Instance);
		await writer.InitializeAsync(columns);
		await writer.WriteBatchAsync(data);
		await writer.CompleteAsync();

		// Assert
		await using var connection = new NpgsqlConnection(connectionString);
		await connection.OpenAsync();
		await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {targetTable}", connection);
		var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());

		Assert.Equal(2, count);
	}
	[Fact]
	public async Task PostgreSqlDataWriter_MixedOrder_MapsCorrectly()
	{
		if (!DockerHelper.IsAvailable() || _postgres is null) return;

		// Arrange
		var connectionString = _postgres.GetConnectionString();
		var tableName = "mixed_order_test";

		// 1. Manually create table with DIFFERENT column order than source data
		// Table: (score, name, id) vs Source: (id, name, score)
		await using (var connection = new NpgsqlConnection(connectionString))
		{
			await connection.OpenAsync();
			await using var cmd = connection.CreateCommand();
			cmd.CommandText = $"DROP TABLE IF EXISTS {tableName}; CREATE TABLE {tableName} (score NUMERIC, name TEXT, id INT)";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Setup Source Data
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
		{
			new("id", typeof(int), false),
			new("name", typeof(string), true),
			new("score", typeof(decimal), false)
		};

		var row1 = new object?[] { 1, "Alice", 95.5m };
		var row2 = new object?[] { 2, "Bob", 80.0m };
		var batch = new List<object?[]> { row1, row2 };

		var writerOptions = new PostgreSqlWriterOptions
		{
			Table = tableName,
			Strategy = PostgreSqlWriteStrategy.Append
		};

		// Act
		await using var writer = new PostgreSqlDataWriter(connectionString, writerOptions, NullLogger<PostgreSqlDataWriter>.Instance);
		await writer.InitializeAsync(columns); // Will find table and append
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// Assert
		await using (var connection = new NpgsqlConnection(connectionString))
		{
			await connection.OpenAsync();
			await using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT id, name, score FROM {tableName} ORDER BY id";
			await using var reader = await cmd.ExecuteReaderAsync();

			Assert.True(await reader.ReadAsync());
			Assert.Equal(1, reader.GetInt32(0)); // Id
			Assert.Equal("Alice", reader.GetString(1)); // Name
			Assert.Equal(95.5m, reader.GetDecimal(2)); // Score

			Assert.True(await reader.ReadAsync());
			Assert.Equal(2, reader.GetInt32(0));
			Assert.Equal("Bob", reader.GetString(1));
			Assert.Equal(80.0m, reader.GetDecimal(2));
		}
	}
	[Fact]
	public async Task PostgreSqlWriter_DeleteThenInsert_ClearsTable()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var targetTable = "test_delete_insert";

		// 1. Create table and insert initial data
		await using (var connection = new NpgsqlConnection(connectionString))
		{
			await connection.OpenAsync();
			await using var cmd = connection.CreateCommand();
			cmd.CommandText = $"DROP TABLE IF EXISTS {targetTable}; CREATE TABLE {targetTable} (id INT, name TEXT)";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = $"INSERT INTO {targetTable} VALUES (999, 'OldData')";
			await cmd.ExecuteNonQueryAsync();
		}

		var options = new PostgreSqlWriterOptions { Table = targetTable, Strategy = PostgreSqlWriteStrategy.DeleteThenInsert };
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
		var data = new List<object?[]> { new object?[] { 1, "NewData" } };

		// Act
		await using var writer = new PostgreSqlDataWriter(connectionString, options, NullLogger<PostgreSqlDataWriter>.Instance);
		await writer.InitializeAsync(columns);
		await writer.WriteBatchAsync(data);
		await writer.CompleteAsync();

		// Assert
		await using (var connection = new NpgsqlConnection(connectionString))
		{
			await connection.OpenAsync();
			await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM {targetTable}", connection);
			var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
			Assert.Equal(1, count); // Only new data

			await using var checkCmd = new NpgsqlCommand($"SELECT id FROM {targetTable}", connection);
			var id = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
			Assert.Equal(1, id);
		}
	}

	[Fact]
	public async Task PostgreSqlWriter_Recreate_DropsAndCreatesTable()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var targetTable = "test_recreate";

		// 1. Create table with incompatible schema (Name as INT)
		await using (var connection = new NpgsqlConnection(connectionString))
		{
			await connection.OpenAsync();
			await using var cmd = connection.CreateCommand();
			cmd.CommandText = $"DROP TABLE IF EXISTS {targetTable}; CREATE TABLE {targetTable} (id INT, name TEXT)"; // Compatible with String
			await cmd.ExecuteNonQueryAsync();
		}

		var options = new PostgreSqlWriterOptions { Table = targetTable, Strategy = PostgreSqlWriteStrategy.Recreate };
		// Source defines Name as String
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
		var data = new List<object?[]> { new object?[] { 1, "NewData" } };

		// Act
		await using var writer = new PostgreSqlDataWriter(connectionString, options, NullLogger<PostgreSqlDataWriter>.Instance);
		await writer.InitializeAsync(columns); // Should Drop and Recreate with correct schema (Name TEXT)
		await writer.WriteBatchAsync(data);
		await writer.CompleteAsync();

		// Assert
		await using (var connection = new NpgsqlConnection(connectionString))
		{
			await connection.OpenAsync();
			// Verify data exists (means write succeeded)
			await using var cmd = new NpgsqlCommand($"SELECT name FROM {targetTable}", connection);
			var name = await cmd.ExecuteScalarAsync() as string;
			Assert.Equal("NewData", name);
		}
	}

	[Fact]
	public async Task PostgreSqlWriter_Recreate_PreservesNativeStructure()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var tableNameRaw = $"test_recreate_p_{Guid.NewGuid():N}".Substring(0, 30); // Lowercase unquoted

		// 1. Manually create table with specific structure:
		// - code: CHAR(10) (Fixed length, unquoted)
		// - "MixedScore": NUMERIC(10,5) (Quoted, mixed case)
		// - description: TEXT
		// - flags: VARCHAR(10) (String representation of bits)
		await using (var connection = new NpgsqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $@"
                CREATE TABLE {tableNameRaw} (
                    code CHAR(10) NOT NULL,
                    ""MixedScore"" NUMERIC(10,5),
                    description TEXT,
                    flags VARCHAR(10),
                    PRIMARY KEY (code)
                )";
			await cmd.ExecuteNonQueryAsync();

			cmd.CommandText = $"INSERT INTO {tableNameRaw} (code, \"MixedScore\", description, flags) VALUES ('OLD', 10.5, 'OldDesc', B'101')";
			await cmd.ExecuteNonQueryAsync();
		}

		var options = new PostgreSqlWriterOptions { Table = tableNameRaw, Strategy = PostgreSqlWriteStrategy.Recreate };

		// Source defines columns generically
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
		{
			new("code", typeof(string), true),
			new("MixedScore", typeof(decimal), false),
			new("description", typeof(string), true),
			new("flags", typeof(string), true)
		};

		var batch = new List<object?[]> { new object?[] { "NEW", 99.12345m, "NewDesc", "010" } }; // "010" string -> BIT(3)

		// Act
		// Recreate should Drop and Re-Create using Introspection
		await using var writer = new PostgreSqlDataWriter(connectionString, options, NullLogger<PostgreSqlDataWriter>.Instance);
		await writer.InitializeAsync(columns);
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// Assert
		// Inspect table structure to verify it matches original, not default
		await using (var connection = new NpgsqlConnection(connectionString))
		{
			await connection.OpenAsync();

			// Check Data
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT code, \"MixedScore\", description, flags::TEXT FROM {tableNameRaw}"; // Cast flags to text for easy reading
			using (var reader = await cmd.ExecuteReaderAsync())
			{
				Assert.True(await reader.ReadAsync());
				Assert.Equal("NEW       ", reader.GetString(0)); // CHAR(10) padded
				Assert.Equal(99.12345m, reader.GetDecimal(1));
				Assert.Equal("NewDesc", reader.GetString(2));
				Assert.Equal("010", reader.GetString(3));
			}

			// Check Metadata using information_schema

			// 1. code (CHAR(10))
			using var metaCode = connection.CreateCommand();
			metaCode.CommandText = $"SELECT data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{tableNameRaw}' AND column_name = 'code'";
			using (var rCode = await metaCode.ExecuteReaderAsync())
			{
				if (!await rCode.ReadAsync())
				{
					throw new Exception($"Column 'code' not found in information_schema.columns for table '{tableNameRaw}'. Check casing.");
				}
				Assert.Equal("character", rCode.GetString(0)); // Postgres often calls CHAR "character"
				Assert.Equal(10, Convert.ToInt32(rCode["character_maximum_length"]));
			}

			// 2. MixedScore (NUMERIC(10,5)) - Preserved case in quotes?
			using var metaScore = connection.CreateCommand();
			metaScore.CommandText = $"SELECT numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = '{tableNameRaw}' AND column_name = 'MixedScore'";
			using (var rScore = await metaScore.ExecuteReaderAsync())
			{
				Assert.True(await rScore.ReadAsync());
				Assert.Equal(10, Convert.ToInt32(rScore["numeric_precision"]));
				Assert.Equal(5, Convert.ToInt32(rScore["numeric_scale"]));
			}

			// 3. flags (BIT(3))
			using var metaFlags = connection.CreateCommand();
			metaFlags.CommandText = $"SELECT data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{tableNameRaw}' AND column_name = 'flags'";
			using (var rFlags = await metaFlags.ExecuteReaderAsync())
			{
				Assert.True(await rFlags.ReadAsync());
				Assert.Equal("character varying", rFlags.GetString(0));
				Assert.Equal(10, Convert.ToInt32(rFlags["character_maximum_length"]));
			}
		}
	}
}
