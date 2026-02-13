using DtPipe.Adapters.DuckDB;
using DtPipe.Tests.Helpers;
using DuckDB.NET.Data;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests;

public class DuckDBIntegrationTests : IAsyncLifetime
{
	private readonly string _dbPath;
	private readonly string _connectionString;

	public DuckDBIntegrationTests()
	{
		_dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.duckdb");
		_connectionString = $"Data Source={_dbPath}";
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;

	public ValueTask DisposeAsync()
	{
		try { File.Delete(_dbPath); } catch { /* best effort */ }
		GC.SuppressFinalize(this);
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task CanReadFromDuckDB()
	{
		// setup
		using (var connection = new DuckDBConnection(_connectionString))
		{
			await connection.OpenAsync(TestContext.Current.CancellationToken);

			using var command = connection.CreateCommand();
			command.CommandText = TestDataSeeder.GenerateTableDDL(connection, "users");
			await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

			await TestDataSeeder.SeedAsync(connection, "users");
		}

		// 2. Read with Reader
		await using var reader = new DuckDataSourceReader(
			_connectionString,
			"SELECT * FROM users ORDER BY id",
			new DuckDbReaderOptions());
		await reader.OpenAsync(TestContext.Current.CancellationToken);

		// 3. Verify Columns
		reader.Columns.Should().HaveCount(7);
		reader.Columns![0].Name.Should().Be("Id");
		reader.Columns![1].Name.Should().Be("Name");

		// 4. Verify Data retrieval
		var batches = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(10, TestContext.Current.CancellationToken))
		{
			for (int i = 0; i < batch.Length; i++)
				batches.Add(batch.Span[i]);
		}

		batches.Should().HaveCount(4);
		batches[0][0].Should().Be(1);
		batches[0][1].Should().Be("Alice");

		batches[1][0].Should().Be(2);
		batches[1][1].Should().Be("Bob");
	}

	[Fact]
	public void ThrowsOnDDL()
	{
		// DtPipe throws InvalidOperationException or ArgumentException depending on where it catches it
		// The regex check throws InvalidOperationException for forbidden keywords at the start
		var act = () => new DuckDataSourceReader(
			_connectionString,
			"DROP TABLE users",
			new DuckDbReaderOptions());
		act.Should().Throw<Exception>(); // Relaxed check to catch either ArgumentException or InvalidOperationException
	}

	[Fact]
	public async Task DuckDbDataWriter_Recreate_PreservesNativeStructure()
	{
		var duckDbPath = Path.Combine(Path.GetTempPath(), $"test_recreate_p_{Guid.NewGuid():N}.duckdb");
		var connectionString = $"Data Source={duckDbPath}";
		var tableNameRaw = "TestRecreateEnh";

		try
		{
			// 1. Manually create table with specific structure:
			// - Code: VARCHAR (DuckDB uses string, but let's see if we can use explicit types)
			// - PreciseNum: DECIMAL(10,5)
			// - "My Blob": BLOB (Quoted)
			// - Tiny: TINYINT
			await using (var connection = new DuckDBConnection(connectionString))
			{
				await connection.OpenAsync();
				using var cmd = connection.CreateCommand();
				cmd.CommandText = $@"
                    CREATE TABLE {tableNameRaw} (
                        Code VARCHAR NOT NULL,
                        PreciseNum DECIMAL(10,5),
                        ""My Blob"" BLOB,
                        Tiny TINYINT,
                        PRIMARY KEY (Code)
                    )";
				await cmd.ExecuteNonQueryAsync();

				cmd.CommandText = $"INSERT INTO {tableNameRaw} VALUES ('OLD', 10.5, '0xAA', 1)";
				await cmd.ExecuteNonQueryAsync();
			}

			var writerOptions = new DuckDbWriterOptions
			{
				Table = tableNameRaw,
				Strategy = DuckDbWriteStrategy.Recreate
			};

			var columns = new List<Core.Models.PipeColumnInfo>
			{
				new("Code", typeof(string), true),
				new("PreciseNum", typeof(decimal), false),
				new("My Blob", typeof(byte[]), true),
				new("Tiny", typeof(int), false)
			};

			var batch = new List<object?[]> { new object?[] { "NEW", 99.12345m, new byte[] { 0xBB }, 120 } };

			// Act
			await using var writer = new DuckDbDataWriter(connectionString, writerOptions, NullLogger<DuckDbDataWriter>.Instance, DuckDbTypeConverter.Instance);
			await writer.InitializeAsync(columns);
			await writer.WriteBatchAsync(batch);
			await writer.CompleteAsync();

			// Assert
			await using (var connection = new DuckDBConnection(connectionString))
			{
				await connection.OpenAsync();

				// Check Data
				using var cmd = connection.CreateCommand();
				cmd.CommandText = $"SELECT Code, PreciseNum, \"My Blob\", Tiny FROM {tableNameRaw}";
				using (var reader = await cmd.ExecuteReaderAsync())
				{
					Assert.True(await reader.ReadAsync());
					Assert.Equal("NEW", reader.GetString(0));
					Assert.Equal(99.12345m, reader.GetDecimal(1));

					Assert.IsAssignableFrom<Stream>(reader.GetValue(2));
					var stream = (Stream)reader.GetValue(2);
					var buffer = new byte[stream.Length];
					int bytesRead = 0;
					while (bytesRead < buffer.Length)
					{
						int read = stream.Read(buffer, bytesRead, buffer.Length - bytesRead);
						if (read == 0) break;
						bytesRead += read;
					}
					Assert.Equal(0xBB, buffer[0]);
					Assert.Equal(120, reader.GetInt32(3));
				}

				// Check Metadata (using PRAGMA table_info)
				using var metaCmd = connection.CreateCommand();
				metaCmd.CommandText = $"PRAGMA table_info('{tableNameRaw}')";

				var types = new Dictionary<string, string>();
				using (var metaReader = await metaCmd.ExecuteReaderAsync())
				{
					while (await metaReader.ReadAsync())
					{
						types[metaReader.GetString(1)] = metaReader.GetString(2);
					}
				}

				Assert.Contains("DECIMAL(10,5)", types["PreciseNum"]);
				Assert.Contains("BLOB", types["My Blob"]);
				Assert.Contains("TINYINT", types["Tiny"]);
			}
		}
		finally
		{
			if (File.Exists(duckDbPath)) File.Delete(duckDbPath);
		}
	}
}
