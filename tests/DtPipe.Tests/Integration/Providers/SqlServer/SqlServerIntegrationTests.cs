using DtPipe.Adapters.Parquet;
using DtPipe.Adapters.SqlServer;
using DtPipe.Tests.Helpers;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// Integration tests using SQL Server Testcontainers.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class SqlServerIntegrationTests : IAsyncLifetime
{
	private readonly DtPipe.Tests.Fixtures.GlobalDatabaseFixture _fixture;
	private string? _connectionString;

	public SqlServerIntegrationTests(DtPipe.Tests.Fixtures.GlobalDatabaseFixture fixture)
	{
		_fixture = fixture;
	}

	public async ValueTask InitializeAsync()
	{
		_connectionString = _fixture.SqlServerConnectionString;

		if (_connectionString == null) return;

		await using var connection = new SqlConnection(_connectionString);
		await connection.OpenAsync();

		// Use Seeder for DDL and Data
		await using var cmd = connection.CreateCommand();
		// Ensure clean state
		cmd.CommandText = "IF OBJECT_ID('test_data', 'U') IS NOT NULL DROP TABLE test_data; " + TestDataSeeder.GenerateTableDDL(connection, "test_data");
		await cmd.ExecuteNonQueryAsync();

		await TestDataSeeder.SeedAsync(connection, "test_data");
	}

	public ValueTask DisposeAsync()
	{
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task SqlServerReader_ReadsAllRows()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;

		// Act
		await using var reader = new SqlServerReader(
			connectionString,
			"SELECT id, name FROM test_data ORDER BY id",
			new SqlServerReaderOptions());

		await reader.OpenAsync(TestContext.Current.CancellationToken);

		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(10, TestContext.Current.CancellationToken))
		{
			for (int i = 0; i < batch.Length; i++)
			{
				rows.Add(batch.Span[i]);
			}
		}

		// Assert
		Assert.Equal(4, rows.Count); // 4 records in test-data.json
		Assert.Equal(2, reader.Columns!.Count); // 2 columns (id, name)
		Assert.Equal("id", reader.Columns[0].Name);

		// Specific data validation (diverse types)
		var alice = rows.First(r => r[0]?.ToString() == "1");
		Assert.Equal("Alice", alice[1]); // Name
	}

	[Fact]
	public async Task ParquetWriter_CreatesValidFile_FromSqlServer()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;
		var outputPath = Path.Combine(Path.GetTempPath(), $"test_sql_{Guid.NewGuid()}.parquet");

		try
		{
			// Act
			await using var reader = new SqlServerReader(
				connectionString,
				"SELECT * FROM test_data ORDER BY Id",
				new SqlServerReaderOptions());

			await reader.OpenAsync(TestContext.Current.CancellationToken);

			await using (var writer = new ParquetDataWriter(outputPath))
			{
				await writer.InitializeAsync(reader.Columns!, TestContext.Current.CancellationToken);

				var rows = new List<object?[]>();
				await foreach (var batchChunk in reader.ReadBatchesAsync(100, TestContext.Current.CancellationToken))
				{
					for (int i = 0; i < batchChunk.Length; i++)
					{
						rows.Add(batchChunk.Span[i]);
					}
				}

				await writer.WriteRecordBatchAsync(ArrowTestHelper.ToRecordBatch(rows, reader.Columns!), TestContext.Current.CancellationToken);
				await writer.CompleteAsync(TestContext.Current.CancellationToken);
			}

			// Assert
			Assert.True(File.Exists(outputPath));
			Assert.True(new FileInfo(outputPath).Length > 0);

			await using var parquetReader = new ParquetStreamReader(outputPath);
			await parquetReader.OpenAsync(TestContext.Current.CancellationToken);

			Assert.Equal(7, parquetReader.Columns!.Count);
			Assert.Equal("id", parquetReader.Columns[0].Name, ignoreCase: true);
			Assert.Equal("name", parquetReader.Columns[1].Name, ignoreCase: true);

			var readRows = new List<object?[]>();
			await foreach (var batchChunk in parquetReader.ReadBatchesAsync(100, TestContext.Current.CancellationToken))
			{
				for (int i = 0; i < batchChunk.Length; i++)
				{
					readRows.Add(batchChunk.Span[i]);
				}
			}

			Assert.Equal(4, readRows.Count);
			var firstRow = readRows.First(r => r[0]?.ToString() == "1");
			Assert.Equal("Alice", firstRow[1]?.ToString());
		}
		finally
		{
			if (File.Exists(outputPath))
				File.Delete(outputPath);
		}
	}
	[Fact]
	public async Task SqlServerDataWriter_MixedOrder_MapsCorrectly()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;
		var tableName = "MixedOrderTest";

		// 1. Manually create table with mixed order: Score (DECIMAL), Name (NVARCHAR), Id (INT)
		// Source will be: Id, Name, Score
		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}; CREATE TABLE {tableName} (Score DECIMAL(18,2), Name NVARCHAR(100), Id INT)";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Setup Source Data
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true),
			new("Score", typeof(decimal), false)
		};

		var row1 = new object?[] { 1, "Alice", 95.5m };
		var row2 = new object?[] { 2, "Bob", 80.0m };
		var batch = new List<object?[]> { row1, row2 };

		var writerOptions = new SqlServerWriterOptions
		{
			Table = tableName,
			Strategy = SqlServerWriteStrategy.Truncate
		};

		// Act
		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance);
		await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		await writer.WriteBatchAsync(batch, TestContext.Current.CancellationToken);
		await writer.CompleteAsync(TestContext.Current.CancellationToken);

		// Assert
		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT Id, Name, Score FROM {tableName} ORDER BY Id";
			using var reader = await cmd.ExecuteReaderAsync();

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
	public async Task SqlServerDataWriter_Recreate_DropsAndCreatesTable()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var tableName = "RecreateTest";

		// 1. Manually create table
		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}; CREATE TABLE {tableName} (Id INT, Name NVARCHAR(100))";
			await cmd.ExecuteNonQueryAsync();
		}

		var writerOptions = new SqlServerWriterOptions { Table = tableName, Strategy = SqlServerWriteStrategy.Recreate };
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) };
		var rows = new List<object?[]> { new object?[] { 1, "NewData" } };

		// Act
		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance);
		await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		await writer.WriteBatchAsync(rows, TestContext.Current.CancellationToken);
		await writer.CompleteAsync(TestContext.Current.CancellationToken);

		// Assert
		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT Name FROM {tableName}";
			var name = await cmd.ExecuteScalarAsync();
			Assert.Equal("NewData", name);
		}
	}

	[Fact]
	public async Task SqlServerDataWriter_Recreate_RebuildsFromTheSourceSchema()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var tableNameRaw = $"TestRecreateEnh_{Guid.NewGuid():N}".Substring(0, 25);

		// 1. Manually create table with specific structure:
		// - Code: NCHAR(10) (Fixed length unicode)
		// - Price: MONEY (Specific type)
		// - Score: DECIMAL(5,2)
		// - "Created At": DATETIME2(3) (Quoted with space, specific precision)
		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $@"
                CREATE TABLE {tableNameRaw} (
                    Code NCHAR(10) NOT NULL,
                    Price MONEY,
                    Score DECIMAL(5,2),
                    ""Created At"" DATETIME2(3),
                    Extra INT,
                    PRIMARY KEY (Code)
                )";
			await cmd.ExecuteNonQueryAsync();

			cmd.CommandText = $"INSERT INTO {tableNameRaw} (Code, Price, Score, \"Created At\", Extra) VALUES ('OLD', 10.50, 10.5, '2023-01-01 12:00:00.123', 5)";
			await cmd.ExecuteNonQueryAsync();
		}

		var writerOptions = new SqlServerWriterOptions
		{
			Table = tableNameRaw,
			Strategy = SqlServerWriteStrategy.Recreate
		};

		var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
		{
			new("Code", typeof(string), true),
			new("Price", typeof(decimal), false),
			new("Score", typeof(decimal), false),
			new("Created At", typeof(DateTime), true)
		};

		var batch = new List<object?[]> { new object?[] { "NEW", 99.99m, 99.9m, new DateTime(2024, 01, 01, 10, 0, 0).AddMilliseconds(999) } };

		// Act
		// Recreate drops the table and builds it from the source columns
		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance);
		try
		{
			await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
			await writer.WriteBatchAsync(batch, TestContext.Current.CancellationToken);
			await writer.CompleteAsync(TestContext.Current.CancellationToken);
		}
		catch (Exception ex)
		{
			Console.Error.WriteLine($"[SQL Server Test Failure]: {ex.Message}");
			if (ex.InnerException != null) Console.Error.WriteLine($"Inner: {ex.InnerException.Message}");
			throw;
		}

		// Assert
		// Inspect table structure to verify it was rebuilt from the source, not from the old table
		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();

			// Check Data
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT Code, Price, Score, \"Created At\" FROM {tableNameRaw}";
			using (var reader = await cmd.ExecuteReaderAsync())
			{
				Assert.True(await reader.ReadAsync());
				Assert.Equal("NEW", reader.GetString(0)); // the source says string, not NCHAR(10)
				Assert.Equal(99.99m, reader.GetDecimal(1));
				Assert.Equal(99.9m, reader.GetDecimal(2));
				Assert.Equal(999, reader.GetDateTime(3).Millisecond);
			}

			// Check Metadata
			// SQL Server: INFORMATION_SCHEMA.COLUMNS
			using var meta = connection.CreateCommand();
			meta.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableNameRaw}'";
			var types = new Dictionary<string, string>();
			using (var r = await meta.ExecuteReaderAsync())
			{
				while (await r.ReadAsync())
					types[r.GetString(0)] = r.GetString(1);
			}

			// The column the source lacks is gone, and no native type survives.
			Assert.Equal(new[] { "Code", "Created At", "Price", "Score" }, types.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
			Assert.NotEqual("nchar", types["Code"]);
			Assert.NotEqual("money", types["Price"]);
		}
	}

	[Fact]
	public async Task SqlServerDataWriter_Recreate_InvalidatesSchemaCache()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var tableNameRaw = $"TestCache_{Guid.NewGuid():N}".Substring(0, 25);

		// 1. Manually create table to ensure it exists
		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"CREATE TABLE {tableNameRaw} (OldCol INT)";
			await cmd.ExecuteNonQueryAsync();
		}

		var writerOptions = new SqlServerWriterOptions
		{
			Table = tableNameRaw,
			Strategy = SqlServerWriteStrategy.Recreate
		};

		var columns = new List<DtPipe.Core.Models.PipeColumnInfo>
		{
			new("NewCol1", typeof(int), false),
			new("NewCol2", typeof(string), true)
		};

		// Act: InitializeAsync will drop, recreate, and invalidate cache
		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance);
		try
		{
			await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);

			// Assert: InspectTargetAsync should fetch fresh columns, not return fake empty list
			var schema = await writer.InspectTargetAsync(TestContext.Current.CancellationToken);

			Assert.NotNull(schema);
			Assert.True(schema!.Exists);
			// The table was rebuilt from the source, so the fresh inspection shows its columns, not OldCol.
			Assert.Equal(new[] { "NewCol1", "NewCol2" }, schema.Columns.Select(c => c.Name).OrderBy(c => c, StringComparer.Ordinal).ToArray());
		}
		finally
		{
			// Cleanup
			await using (var connection = new SqlConnection(connectionString))
			{
				await connection.OpenAsync();
				using var cmd = connection.CreateCommand();
				cmd.CommandText = $"IF OBJECT_ID('{tableNameRaw}', 'U') IS NOT NULL DROP TABLE {tableNameRaw}";
				await cmd.ExecuteNonQueryAsync();
			}
		}
	}

	[Fact]
	public async Task SqlServerDataWriter_DeleteThenInsert_ClearsAndInserts()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var tableName = $"TestDelIns_{Guid.NewGuid():N}".Substring(0, 25);

		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"CREATE TABLE {tableName} (Id INT, Name NVARCHAR(100))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = $"INSERT INTO {tableName} VALUES (1, 'Old1'), (2, 'Old2')";
			await cmd.ExecuteNonQueryAsync();
		}

		var writerOptions = new SqlServerWriterOptions { Table = tableName, Strategy = SqlServerWriteStrategy.DeleteThenInsert };
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) };
		var rows = new List<object?[]> { new object?[] { 3, "NewData" } };

		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance);
		try
		{
			await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
			await writer.WriteBatchAsync(rows, TestContext.Current.CancellationToken);
			await writer.CompleteAsync(TestContext.Current.CancellationToken);

			await using (var connection = new SqlConnection(connectionString))
			{
				await connection.OpenAsync();
				using var cmd = connection.CreateCommand();
				cmd.CommandText = $"SELECT Id, Name FROM {tableName} ORDER BY Id";
				using var reader = await cmd.ExecuteReaderAsync();

				Assert.True(await reader.ReadAsync());
				Assert.Equal(3, reader.GetInt32(0));
				Assert.Equal("NewData", reader.GetString(1));
				Assert.False(await reader.ReadAsync()); // Only 1 row remains
			}
		}
		finally
		{
			await using (var connection = new SqlConnection(connectionString))
			{
				await connection.OpenAsync();
				using var cmd = connection.CreateCommand();
				cmd.CommandText = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}";
				await cmd.ExecuteNonQueryAsync();
			}
		}
	}

	[Fact]
	public async Task SqlServerDataWriter_Append_AddsToTable()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		var connectionString = _connectionString;
		var tableName = $"TestAppend_{Guid.NewGuid():N}".Substring(0, 25);

		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"CREATE TABLE {tableName} (Id INT, Name NVARCHAR(100))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = $"INSERT INTO {tableName} VALUES (1, 'Old1'), (2, 'Old2')";
			await cmd.ExecuteNonQueryAsync();
		}

		var writerOptions = new SqlServerWriterOptions { Table = tableName, Strategy = SqlServerWriteStrategy.Append };
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) };
		var rows = new List<object?[]> { new object?[] { 3, "NewData" } };

		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance, SqlServerTypeConverter.Instance);
		try
		{
			await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
			await writer.WriteBatchAsync(rows, TestContext.Current.CancellationToken);
			await writer.CompleteAsync(TestContext.Current.CancellationToken);

			await using (var connection = new SqlConnection(connectionString))
			{
				await connection.OpenAsync();
				using var cmd = connection.CreateCommand();
				cmd.CommandText = $"SELECT Id, Name FROM {tableName} ORDER BY Id";
				using var reader = await cmd.ExecuteReaderAsync();

				Assert.True(await reader.ReadAsync());
				Assert.Equal(1, reader.GetInt32(0));
				Assert.True(await reader.ReadAsync());
				Assert.Equal(2, reader.GetInt32(0));
				Assert.True(await reader.ReadAsync());
				Assert.Equal(3, reader.GetInt32(0));
				Assert.Equal("NewData", reader.GetString(1));
				Assert.False(await reader.ReadAsync());
			}
		}
		finally
		{
			await using (var connection = new SqlConnection(connectionString))
			{
				await connection.OpenAsync();
				using var cmd = connection.CreateCommand();
				cmd.CommandText = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}";
				await cmd.ExecuteNonQueryAsync();
			}
		}
	}
}

