using DtPipe.Adapters.Parquet;
using DtPipe.Adapters.SqlServer;
using DtPipe.Tests.Helpers;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.MsSql;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// Integration tests using SQL Server Testcontainers.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class SqlServerIntegrationTests : IAsyncLifetime
{
	private MsSqlContainer? _sqlServer;
	private string? _connectionString;

	public async ValueTask InitializeAsync()
	{
		_connectionString = await DockerHelper.GetSqlServerConnectionString(async () =>
		{
			_sqlServer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
			await _sqlServer.StartAsync();
			return _sqlServer.GetConnectionString();
		});

		if (_connectionString == null) return;

		await using var connection = new SqlConnection(_connectionString);
		await connection.OpenAsync();

		// Use Seeder for DDL and Data
		await using var cmd = connection.CreateCommand();
		cmd.CommandText = "IF OBJECT_ID('test_data', 'U') IS NOT NULL DROP TABLE test_data; " + TestDataSeeder.GenerateTableDDL(connection, "test_data");
		await cmd.ExecuteNonQueryAsync();

		await TestDataSeeder.SeedAsync(connection, "test_data");
	}

	public async ValueTask DisposeAsync()
	{
		if (_sqlServer is not null)
		{
			await _sqlServer.DisposeAsync();
		}
	}

	[Fact]
	public async Task SqlServerStreamReader_ReadsAllRows()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;

		// Act
		await using var reader = new SqlServerStreamReader(
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
			await using var reader = new SqlServerStreamReader(
				connectionString,
				"SELECT * FROM test_data ORDER BY Id",
				new SqlServerReaderOptions());

			await reader.OpenAsync(TestContext.Current.CancellationToken);

			await using var writer = new ParquetDataWriter(outputPath);
			await writer.InitializeAsync(reader.Columns!, TestContext.Current.CancellationToken);

			var rows = new List<object?[]>();
			await foreach (var batchChunk in reader.ReadBatchesAsync(100, TestContext.Current.CancellationToken))
			{
				for (int i = 0; i < batchChunk.Length; i++)
				{
					rows.Add(batchChunk.Span[i]);
				}
			}

			await writer.WriteBatchAsync(rows, TestContext.Current.CancellationToken);
			await writer.CompleteAsync(TestContext.Current.CancellationToken);

			// Assert
			Assert.True(File.Exists(outputPath));
			Assert.True(new FileInfo(outputPath).Length > 0);
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
		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance);
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
		if (!DockerHelper.IsAvailable() || _sqlServer is null) return;

		var connectionString = _sqlServer.GetConnectionString();
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
		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance);
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
	public async Task SqlServerDataWriter_Recreate_PreservesNativeStructure()
	{
		if (!DockerHelper.IsAvailable() || _sqlServer is null) return;

		var connectionString = _sqlServer.GetConnectionString();
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
                    PRIMARY KEY (Code)
                )";
			await cmd.ExecuteNonQueryAsync();

			cmd.CommandText = $"INSERT INTO {tableNameRaw} (Code, Price, Score, \"Created At\") VALUES ('OLD', 10.50, 10.5, '2023-01-01 12:00:00.123')";
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
		// Recreate should Drop and Re-Create using Introspection
		await using var writer = new SqlServerDataWriter(connectionString, writerOptions, NullLogger<SqlServerDataWriter>.Instance);
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
		// Inspect table structure to verify it matches original, not default
		await using (var connection = new SqlConnection(connectionString))
		{
			await connection.OpenAsync();

			// Check Data
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT Code, Price, Score, \"Created At\" FROM {tableNameRaw}";
			using (var reader = await cmd.ExecuteReaderAsync())
			{
				Assert.True(await reader.ReadAsync());
				Assert.Equal("NEW       ", reader.GetString(0)); // NCHAR padded
				Assert.Equal(99.99m, reader.GetDecimal(1));
				Assert.Equal(99.9m, reader.GetDecimal(2));
				Assert.Equal(999, reader.GetDateTime(3).Millisecond);
			}

			// Check Metadata
			// SQL Server: INFORMATION_SCHEMA.COLUMNS

			// 1. Code (NCHAR(10))
			using var metaCode = connection.CreateCommand();
			metaCode.CommandText = $"SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableNameRaw}' AND COLUMN_NAME = 'Code'";
			using (var rCode = await metaCode.ExecuteReaderAsync())
			{
				Assert.True(await rCode.ReadAsync());
				Assert.Equal("nchar", rCode.GetString(0));
				Assert.Equal(10, Convert.ToInt32(rCode["CHARACTER_MAXIMUM_LENGTH"]));
			}

			// 2. Price (MONEY)
			using var metaPrice = connection.CreateCommand();
			metaPrice.CommandText = $"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableNameRaw}' AND COLUMN_NAME = 'Price'";
			var typePrice = await metaPrice.ExecuteScalarAsync();
			Assert.Equal("money", typePrice);

			// 3. Score (DECIMAL(5,2))
			using var scoreCmd = connection.CreateCommand();
			scoreCmd.CommandText = $"SELECT NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableNameRaw}' AND COLUMN_NAME = 'Score'";
			using (var scoreReader = await scoreCmd.ExecuteReaderAsync())
			{
				Assert.True(await scoreReader.ReadAsync());
				Assert.Equal(5, Convert.ToInt32(scoreReader["NUMERIC_PRECISION"]));
				Assert.Equal(2, Convert.ToInt32(scoreReader["NUMERIC_SCALE"]));
			}

			// 4. "Created At" (DATETIME2(3))
			using var metaDate = connection.CreateCommand();
			metaDate.CommandText = $"SELECT DATA_TYPE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableNameRaw}' AND COLUMN_NAME = 'Created At'";
			using (var rDate = await metaDate.ExecuteReaderAsync())
			{
				Assert.True(await rDate.ReadAsync());
				Assert.Equal("datetime2", rDate.GetString(0));
				Assert.Equal(3, Convert.ToInt16(rDate["DATETIME_PRECISION"]));
			}
		}
	}
}
