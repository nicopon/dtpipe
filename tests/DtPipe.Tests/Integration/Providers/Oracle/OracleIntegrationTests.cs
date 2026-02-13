using DtPipe.Adapters.Csv;
using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.Oracle;
using DtPipe.Adapters.Parquet;
using DtPipe.Core.Models;
using DtPipe.Tests.Helpers;
using Oracle.ManagedDataAccess.Client;
using Testcontainers.Oracle;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// Integration tests using Oracle Testcontainers.
/// Requires Docker to be running.
/// </summary>
[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class OracleIntegrationTests : IAsyncLifetime
{
	private OracleContainer? _oracle;
	private string? _connectionString;

	public async ValueTask InitializeAsync()
	{
		_connectionString = await DockerHelper.GetOracleConnectionString(async () =>
		{
			_oracle = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart").Build();
			await _oracle.StartAsync();
			return _oracle.GetConnectionString();
		});

		if (_connectionString == null) return;

		await using var connection = new OracleConnection(_connectionString);
		await connection.OpenAsync();

		// Use Seeder for DDL and Data
		await using var cmd = connection.CreateCommand();
		cmd.CommandText = "BEGIN EXECUTE IMMEDIATE 'DROP TABLE test_data'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
		await cmd.ExecuteNonQueryAsync();
		cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "test_data");
		await cmd.ExecuteNonQueryAsync();

		await TestDataSeeder.SeedAsync(connection, "test_data");
	}

	public async ValueTask DisposeAsync()
	{
		if (_oracle is not null)
		{
			await _oracle.DisposeAsync();
		}
	}

	[Fact]
	public async Task OracleStreamReader_ReadsAllRows()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;

		// Act
		await using var reader = new OracleStreamReader(
			connectionString,
			"SELECT id, name FROM test_data ORDER BY id",
			new OracleReaderOptions { FetchSize = 65536 });

		await reader.OpenAsync(TestContext.Current.CancellationToken);

		var rows = new List<object?[]>();
		await foreach (var row in reader.ReadRowsAsync(TestContext.Current.CancellationToken))
		{
			rows.Add(row);
		}

		// Assert
		Assert.Equal(4, rows.Count);
		Assert.Equal(2, reader.Columns!.Count);
		Assert.Equal("ID", reader.Columns[0].Name);
	}

	[Fact]
	public async Task ParquetWriter_CreatesValidFile()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;
		var outputPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.parquet");

		try
		{
			// Act
			await using var reader = new OracleStreamReader(
				connectionString,
				"SELECT id, name FROM test_data ORDER BY id",
				new OracleReaderOptions { FetchSize = 65536 });

			await reader.OpenAsync(TestContext.Current.CancellationToken);

			await using var writer = new ParquetDataWriter(outputPath);
			await writer.InitializeAsync(reader.Columns!, TestContext.Current.CancellationToken);

			var batch = new List<object?[]>();
			await foreach (var row in reader.ReadRowsAsync(TestContext.Current.CancellationToken))
			{
				batch.Add(row);
			}

			await writer.WriteBatchAsync(batch, TestContext.Current.CancellationToken);
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
	public async Task CsvWriter_CreatesValidFile()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;
		var outputPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.csv");

		try
		{
			// Act - write data
			await using (var reader = new OracleStreamReader(
				connectionString,
				"SELECT id, name FROM test_data ORDER BY id",
				new OracleReaderOptions { FetchSize = 65536 }))
			{
				await reader.OpenAsync(TestContext.Current.CancellationToken);

				await using (var writer = new CsvDataWriter(outputPath))
				{
					await writer.InitializeAsync(reader.Columns!, TestContext.Current.CancellationToken);

					var batch = new List<object?[]>();
					await foreach (var row in reader.ReadRowsAsync(TestContext.Current.CancellationToken))
					{
						batch.Add(row);
					}

					await writer.WriteBatchAsync(batch, TestContext.Current.CancellationToken);
					await writer.CompleteAsync(TestContext.Current.CancellationToken);
				}
			}

			// Assert - after writer is disposed
			Assert.True(File.Exists(outputPath));
			var lines = await File.ReadAllLinesAsync(outputPath, TestContext.Current.CancellationToken);
			Assert.Equal(5, lines.Length); // Header + 4 data rows
		}
		finally
		{
			if (File.Exists(outputPath))
				File.Delete(outputPath);
		}
	}

	[Fact]
	public async Task OracleDataWriter_BulkCopy_FromDuckDb_ToOracle()
	{
		if (!DockerHelper.IsAvailable() || _oracle is null) return;

		// Arrange: Create in-memory DuckDB source with test data
		var duckDbPath = Path.Combine(Path.GetTempPath(), $"test_source_{Guid.NewGuid()}.duckdb");
		var duckConnectionString = $"Data Source={duckDbPath}";

		try
		{
			// Seed DuckDB source
			await using (var duckConnection = new DuckDB.NET.Data.DuckDBConnection(duckConnectionString))
			{
				await duckConnection.OpenAsync(TestContext.Current.CancellationToken);
				await using var cmd = duckConnection.CreateCommand();
				cmd.CommandText = TestDataSeeder.GenerateTableDDL(duckConnection, "source_data");
				await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
				await TestDataSeeder.SeedAsync(duckConnection, "source_data");
			}

			// Act: Read from DuckDB, write to Oracle
			var targetTable = $"TARGET_{Guid.NewGuid():N}".Substring(0, 20).ToUpperInvariant();

			await using var reader = new DuckDataSourceReader(
				duckConnectionString,
				"SELECT Id, Name, Score FROM source_data ORDER BY Id",
				new DuckDbReaderOptions());

			await reader.OpenAsync(TestContext.Current.CancellationToken);
			var columns = reader.Columns!;

			var writerOptions = new OracleWriterOptions
			{
				Table = targetTable,
				Strategy = OracleWriteStrategy.Recreate
			};

			await using var writer = new OracleDataWriter(_oracle.GetConnectionString(), writerOptions, Microsoft.Extensions.Logging.Abstractions.NullLogger<OracleDataWriter>.Instance);
			await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);

			// Read all rows and write in batches
			await foreach (var batch in reader.ReadBatchesAsync(100, TestContext.Current.CancellationToken))
			{
				await writer.WriteBatchAsync(batch.ToArray(), TestContext.Current.CancellationToken);
			}

			await writer.CompleteAsync(TestContext.Current.CancellationToken);

			// Assert: Verify data in Oracle
			await using var verifyConnection = new OracleConnection(_oracle.GetConnectionString());
			await verifyConnection.OpenAsync(TestContext.Current.CancellationToken);

			await using var countCmd = verifyConnection.CreateCommand();
			countCmd.CommandText = $"SELECT COUNT(*) FROM \"{targetTable}\"";
			var count = Convert.ToInt32(await countCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
			Assert.Equal(4, count); // 4 rows from test-data.json

			// Verify sample data integrity
			await using var selectCmd = verifyConnection.CreateCommand();
			selectCmd.CommandText = $"SELECT \"Name\", \"Score\" FROM \"{targetTable}\" WHERE \"Id\" = 1";
			await using var resultReader = await selectCmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
			Assert.True(await resultReader.ReadAsync(TestContext.Current.CancellationToken));
			Assert.Equal("Alice", resultReader.GetString(0));
			Assert.Equal(95.50m, resultReader.GetDecimal(1));
		}
		finally
		{
			if (File.Exists(duckDbPath))
				File.Delete(duckDbPath);
		}
	}

	[Fact]
	public async Task OracleDataWriter_BulkCopy_WithDifferentColumnOrder_MapsCorrectly()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;
		var tableName = $"TEST_MAPPING_{Guid.NewGuid():N}".Substring(0, 20).ToUpperInvariant();

		// 1. Manually create target table with mixed order: Name (VARCHAR), Score (NUMBER), Id (NUMBER)
		// Source will be: Id, Name, Score
		await using (var connection = new OracleConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tableName}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
			await cmd.ExecuteNonQueryAsync();
			// Oracle default is uppercase.
			cmd.CommandText = $"CREATE TABLE {tableName} (\"Name\" VARCHAR2(100), \"Score\" NUMBER(10,2), \"Id\" NUMBER(10))";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Setup Source Data
		var columns = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false, true),
			new("Name", typeof(string), true, true),
			new("Score", typeof(decimal), false, true)
		};

		var row1 = new object?[] { 1, "Alice", 95.5m };
		var row2 = new object?[] { 2, "Bob", 80.0m };
		var batch = new List<object?[]> { row1, row2 };

		var writerOptions = new OracleWriterOptions
		{
			Table = tableName,
			Strategy = OracleWriteStrategy.Truncate, // Or Append
		};

		// Act
		// Use NullLogger to avoid test output noise
		await using var writer = new OracleDataWriter(connectionString, writerOptions, Microsoft.Extensions.Logging.Abstractions.NullLogger<OracleDataWriter>.Instance);
		await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		await writer.WriteBatchAsync(batch, TestContext.Current.CancellationToken);
		await writer.CompleteAsync(TestContext.Current.CancellationToken);

		// Assert
		await using (var connection = new OracleConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT \"Id\", \"Name\", \"Score\" FROM {tableName} ORDER BY \"Id\"";
			using var reader = await cmd.ExecuteReaderAsync();

			Assert.True(await reader.ReadAsync());
			Assert.Equal(1, Convert.ToInt32(reader["Id"])); // Should be 1, not "Alice" or 95.5
			Assert.Equal("Alice", reader["Name"]);
			Assert.Equal(95.5m, Convert.ToDecimal(reader["Score"]));

			Assert.True(await reader.ReadAsync());
			Assert.Equal(2, Convert.ToInt32(reader["Id"]));
			Assert.Equal("Bob", reader["Name"]);
			Assert.Equal(80.0m, Convert.ToDecimal(reader["Score"]));
		}
	}

	[Fact]
	public async Task OracleDataWriter_Recreate_DropsAndCreatesTable()
	{
		if (!DockerHelper.IsAvailable() || _connectionString is null) return;

		// Arrange
		var connectionString = _connectionString;
		var tableName = ($"TEST_RECREATE_{Guid.NewGuid():N}").Substring(0, 20).ToUpperInvariant();

		// 1. Manually create table with incompatible schema (Name as NUMBER) to prove drop happened
		await using (var connection = new OracleConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {tableName}'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = $"CREATE TABLE {tableName} (\"Id\" NUMBER(10), \"Name\" VARCHAR2(100))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = $"INSERT INTO {tableName} VALUES (1, 100)";
			await cmd.ExecuteNonQueryAsync();
		}

		var writerOptions = new OracleWriterOptions
		{
			Table = tableName,
			Strategy = OracleWriteStrategy.Recreate
		};

		// Source defines Name as String
		var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false), new("Name", typeof(string), true) };
		var rows = new List<object?[]> { new object?[] { 1, "NewData" } };

		// Act
		await using var writer = new OracleDataWriter(connectionString, writerOptions, Microsoft.Extensions.Logging.Abstractions.NullLogger<OracleDataWriter>.Instance);
		await writer.InitializeAsync(columns, TestContext.Current.CancellationToken); // Should Drop and Recreate
		await writer.WriteBatchAsync(rows, TestContext.Current.CancellationToken);
		await writer.CompleteAsync(TestContext.Current.CancellationToken);

		// Assert
		await using (var connection = new OracleConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT * FROM {tableName}";
			using (var reader = await cmd.ExecuteReaderAsync())
			{
				Assert.True(await reader.ReadAsync());
				Assert.Equal("NewData", reader.GetString(1)); // Name is the second column (Id, Name)
			}
		}
	}

	[Fact]
	public async Task OracleDataWriter_Recreate_PreservesNativeStructure()
	{
		if (!DockerHelper.IsAvailable() || _oracle is null) return;

		var connectionString = _oracle.GetConnectionString();
		// Use unquoted name to verify case handling (Oracle uppercases this)
		var tableNamePrefix = "TEST_REC_P_";
		var tableNameRaw = $"{tableNamePrefix}{Guid.NewGuid():N}".Substring(0, 25);

		// 1. Manually create table with specific structure AND data
		// We use a mix of quoted and unquoted identifiers to test robustness
		// - Code: CHAR(10) - Fixed length, unquoted (becomes CODE)
		// - "MixedCase": VARCHAR2(50) - Quoted, specific case
		// - Score: NUMBER(7,3) - Specific precision/scale
		// - Flag: CHAR(1) - Boolean-ish
		// - GenericNum: NUMBER - No precision (should be preserved as such)
		await using (var connection = new OracleConnection(connectionString))
		{
			await connection.OpenAsync();
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $@"
                CREATE TABLE {tableNameRaw} (
                    Code CHAR(10) NOT NULL,
                    ""MixedCase"" VARCHAR2(50),
                    Score NUMBER(7,3),
                    Flag CHAR(1),
                    GenericNum NUMBER,
                    PRIMARY KEY (Code)
                )";
			await cmd.ExecuteNonQueryAsync();

			// Insert initial data
			cmd.CommandText = $"INSERT INTO {tableNameRaw} (Code, \"MixedCase\", Score, Flag, GenericNum) VALUES ('OLD', 'OldVal', 10.123, 'Y', 12345)";
			await cmd.ExecuteNonQueryAsync();
		}

		var writerOptions = new OracleWriterOptions
		{
			Table = tableNameRaw, // Pass unquoted, writer should resolve it to TEST_REC_P_...
			Strategy = OracleWriteStrategy.Recreate
		};

		// Source defines columns with simple types, we expect the writer to ignore these and use the existing schema
		var columns = new List<PipeColumnInfo>
		{
			new("Code", typeof(string), true),
			new("MixedCase", typeof(string), true),
			new("Score", typeof(decimal), false),
			new("Flag", typeof(string), true), // Source says string, target is CHAR(1)
            new("GenericNum", typeof(decimal), true)
		};

		var rows = new List<object?[]> { new object?[] { "NEW", "NewVal", 99.999m, "N", 67890m } };

		// Act
		// Recreate should Drop and Re-Create using Introspection
		await using var writer = new OracleDataWriter(connectionString, writerOptions, Microsoft.Extensions.Logging.Abstractions.NullLogger<OracleDataWriter>.Instance);
		await writer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		await writer.WriteBatchAsync(rows, TestContext.Current.CancellationToken);
		await writer.CompleteAsync(TestContext.Current.CancellationToken);

		// Assert
		// Inspect table structure to verify it matches original, not default
		await using (var connection = new OracleConnection(connectionString))
		{
			await connection.OpenAsync();

			// Check Data
			using var cmd = connection.CreateCommand();
			cmd.CommandText = $"SELECT Code, \"MixedCase\", Score, Flag, GenericNum FROM {tableNameRaw}";
			using (var reader = await cmd.ExecuteReaderAsync())
			{
				Assert.True(await reader.ReadAsync());
				Assert.Equal("NEW       ", reader.GetString(0)); // CHAR(10) is padded! Verification of CHAR type.
				Assert.Equal("NewVal", reader.GetString(1));
				Assert.Equal(99.999m, reader.GetDecimal(2));
				Assert.Equal("N", reader.GetString(3));
				Assert.Equal(67890m, reader.GetDecimal(4));
			}

			// Check Metadata using Oracle system views
			// Note: USER_TAB_COLUMNS stores names in UPPERCASE unless quoted during creation

			// 1. Check CHAR(10) - (stored as CODE)
			using var metaCode = connection.CreateCommand();
			metaCode.CommandText = $"SELECT DATA_TYPE, CHAR_LENGTH FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER('{tableNameRaw}') AND COLUMN_NAME = 'CODE'";
			using (var rCode = await metaCode.ExecuteReaderAsync())
			{
				Assert.True(await rCode.ReadAsync());
				Assert.Equal("CHAR", rCode.GetString(0));
				Assert.Equal(10, Convert.ToInt32(rCode["CHAR_LENGTH"]));
			}

			// 2. Check NUMBER(7,3) - (stored as SCORE)
			using var metaScore = connection.CreateCommand();
			metaScore.CommandText = $"SELECT DATA_PRECISION, DATA_SCALE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER('{tableNameRaw}') AND COLUMN_NAME = 'SCORE'";
			using (var rScore = await metaScore.ExecuteReaderAsync())
			{
				Assert.True(await rScore.ReadAsync());
				Assert.Equal(7, Convert.ToInt32(rScore["DATA_PRECISION"]));
				Assert.Equal(3, Convert.ToInt32(rScore["DATA_SCALE"]));
			}

			// 3. Check VARCHAR2(50) for MixedCase column - (stored as MixedCase because it was quoted)
			using var metaMixed = connection.CreateCommand();
			metaMixed.CommandText = $"SELECT CHAR_LENGTH FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER('{tableNameRaw}') AND COLUMN_NAME = 'MixedCase'";
			var len = await metaMixed.ExecuteScalarAsync();
			Assert.Equal(50, Convert.ToInt32(len));

			// 4. Check GenericNum - NUMBER (no precision)
			using var metaGen = connection.CreateCommand();
			metaGen.CommandText = $"SELECT DATA_PRECISION, DATA_SCALE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER('{tableNameRaw}') AND COLUMN_NAME = 'GENERICNUM'";
			using (var rGen = await metaGen.ExecuteReaderAsync())
			{
				Assert.True(await rGen.ReadAsync());
				// When NUMBER is defined without precision, PRECISION/SCALE are often null in metadata
				Assert.True(rGen.IsDBNull(0) || rGen.IsDBNull(1));
			}
		}
	}
}
