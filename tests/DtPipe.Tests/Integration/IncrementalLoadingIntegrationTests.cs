using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.Oracle;
using DtPipe.Adapters.PostgreSQL;
using DtPipe.Adapters.Sqlite;
using DtPipe.Adapters.SqlServer;
using DtPipe.Tests.Helpers;
using DuckDB.NET.Data;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Testcontainers.MsSql;
using Testcontainers.Oracle;
using Testcontainers.PostgreSql;
using Xunit;

namespace DtPipe.Tests.Integration;

public class IncrementalLoadingIntegrationTests : IAsyncLifetime
{
	private readonly string _dbPath;
	private readonly string _connectionString;

	public IncrementalLoadingIntegrationTests()
	{
		_dbPath = Path.Combine(Path.GetTempPath(), $"inc_test_{Guid.NewGuid()}.duckdb");
		_connectionString = $"Data Source={_dbPath}";
	}

	public ValueTask InitializeAsync() => ValueTask.CompletedTask;
	public ValueTask DisposeAsync()
	{
		try { File.Delete(_dbPath); } catch { }
		GC.SuppressFinalize(this);
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task DuckDB_Upsert_UpdatesExisting_InsertsNew()
	{
		// 1. Setup Table with Data
		await using var connection = new DuckDBConnection(_connectionString);
		await connection.OpenAsync();
		using var cmd = connection.CreateCommand();
		cmd.CommandText = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)";
		await cmd.ExecuteNonQueryAsync();
		cmd.CommandText = "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')";
		await cmd.ExecuteNonQueryAsync();

		// 2. Prepare Writer
		var options = new DuckDbWriterOptions
		{
			Table = "users",
			Strategy = DuckDbWriteStrategy.Upsert,
			Key = "id"
		};
		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);

		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			new("id", typeof(int), false),
			new("name", typeof(string), true)
		};
		await writer.InitializeAsync(columns);

		// 3. Write Batch (1 Updated, 3 New)
		// Update Alice -> Alice V2
		// Insert Charlie
		var batch = new List<object?[]> {
			new object[] { 1, "Alice V2" },
			new object[] { 3, "Charlie" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 4. Verify
		var result = await QueryAll("SELECT id, name FROM users ORDER BY id");
		result.Should().HaveCount(3);
		result[0][1].Should().Be("Alice V2");
		result[1][1].Should().Be("Bob");
		result[2][1].Should().Be("Charlie");
	}

	[Fact]
	public async Task DuckDB_Ignore_SkipsExisting_InsertsNew()
	{
		// 1. Setup Table with Data
		await using var connection = new DuckDBConnection(_connectionString);
		await connection.OpenAsync();
		using var cmd = connection.CreateCommand();
		cmd.CommandText = "CREATE TABLE users_ign (id INT PRIMARY KEY, name TEXT)";
		await cmd.ExecuteNonQueryAsync();
		cmd.CommandText = "INSERT INTO users_ign VALUES (1, 'Alice'), (2, 'Bob')";
		await cmd.ExecuteNonQueryAsync();

		// 2. Prepare Writer
		var options = new DuckDbWriterOptions
		{
			Table = "users_ign",
			Strategy = DuckDbWriteStrategy.Ignore,
			Key = "id"
		};
		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);

		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			new("id", typeof(int), false),
			new("name", typeof(string), true)
		};
		await writer.InitializeAsync(columns);

		// 3. Write Batch
		// Update Alice -> Alice V2 (Should be ignored)
		// Insert Charlie
		var batch = new List<object?[]> {
			new object[] { 1, "Alice V2" }, // Should be ignored
            new object[] { 3, "Charlie" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 4. Verify
		var result = await QueryAll("SELECT id, name FROM users_ign ORDER BY id");
		result.Should().HaveCount(3);
		result[0][1].Should().Be("Alice"); // Unchanged
		result[1][1].Should().Be("Bob");
		result[2][1].Should().Be("Charlie");
	}

	private async Task<List<object[]>> QueryAll(string sql)
	{
		var list = new List<object[]>();
		await using var conn = new DuckDBConnection(_connectionString);
		await conn.OpenAsync();
		using var cmd = conn.CreateCommand();
		cmd.CommandText = sql;
		using var reader = await cmd.ExecuteReaderAsync();
		while (await reader.ReadAsync())
		{
			var row = new object[reader.FieldCount];
			reader.GetValues(row);
			list.Add(row);
		}
		return list;
	}

	[Fact]
	public async Task SqlServer_Upsert_UpdatesExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;

		var cs = await DockerHelper.GetSqlServerConnectionString(async () =>
		{
			var container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		// 1. Setup
		await using (var conn = new SqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "IF OBJECT_ID('users', 'U') IS NOT NULL DROP TABLE users; CREATE TABLE users (id INT PRIMARY KEY, name NVARCHAR(100))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Write
		var options = new SqlServerWriterOptions { Table = "users", Strategy = SqlServerWriteStrategy.Upsert, Key = "id" };
		await using var writer = new SqlServerDataWriter(cs, options, NullLogger<SqlServerDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> {
			new object[] { 1, "Alice V2" },
			new object[] { 3, "Charlie" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 3. Verify
		await using (var conn = new SqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT id, name FROM users ORDER BY id";
			using var reader = await cmd.ExecuteReaderAsync();

			Assert.True(await reader.ReadAsync());
			Assert.Equal("Alice V2", reader["name"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Bob", reader["name"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Charlie", reader["name"]);
		}
	}

	[Fact]
	public async Task SqlServer_Ignore_SkipsExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;

		var cs = await DockerHelper.GetSqlServerConnectionString(async () =>
		{
			var container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		// 1. Setup
		await using (var conn = new SqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "IF OBJECT_ID('users_ign', 'U') IS NOT NULL DROP TABLE users_ign; CREATE TABLE users_ign (id INT PRIMARY KEY, name NVARCHAR(100))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO users_ign VALUES (1, 'Alice'), (2, 'Bob')";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Write
		var options = new SqlServerWriterOptions { Table = "users_ign", Strategy = SqlServerWriteStrategy.Ignore, Key = "id" };
		await using var writer = new SqlServerDataWriter(cs, options, NullLogger<SqlServerDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> {
			new object[] { 1, "Alice V2" }, // Should be ignored
            new object[] { 3, "Charlie" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 3. Verify
		await using (var conn = new SqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT id, name FROM users_ign ORDER BY id";
			using var reader = await cmd.ExecuteReaderAsync();

			Assert.True(await reader.ReadAsync());
			Assert.Equal("Alice", reader["name"]); // Unchanged
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Bob", reader["name"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Charlie", reader["name"]);
		}
	}

	[Fact]
	public async Task PostgreSql_Upsert_UpdatesExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;

		var cs = await DockerHelper.GetPostgreSqlConnectionString(async () =>
		{
			var container = new PostgreSqlBuilder("postgres:15-alpine").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		// 1. Setup
		await using (var conn = new NpgsqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "DROP TABLE IF EXISTS users; CREATE TABLE users (id INT PRIMARY KEY, name TEXT)";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Write
		var options = new PostgreSqlWriterOptions { Table = "users", Strategy = PostgreSqlWriteStrategy.Upsert, Key = "id" };
		await using var writer = new PostgreSqlDataWriter(cs, options, NullLogger<PostgreSqlDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> {
			new object[] { 1, "Alice V2" },
			new object[] { 3, "Charlie" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 3. Verify
		await using (var conn = new NpgsqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT id, name FROM users ORDER BY id";
			using var reader = await cmd.ExecuteReaderAsync();

			Assert.True(await reader.ReadAsync());
			Assert.Equal("Alice V2", reader["name"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Bob", reader["name"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Charlie", reader["name"]);
		}
	}

	[Fact]
	public async Task PostgreSql_Ignore_SkipsExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;

		var cs = await DockerHelper.GetPostgreSqlConnectionString(async () =>
		{
			var container = new PostgreSqlBuilder("postgres:15-alpine").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		// 1. Setup
		await using (var conn = new NpgsqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "DROP TABLE IF EXISTS users_ign; CREATE TABLE users_ign (id INT PRIMARY KEY, name TEXT)";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO users_ign VALUES (1, 'Alice'), (2, 'Bob')";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Write
		var options = new PostgreSqlWriterOptions { Table = "users_ign", Strategy = PostgreSqlWriteStrategy.Ignore, Key = "id" };
		await using var writer = new PostgreSqlDataWriter(cs, options, NullLogger<PostgreSqlDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> {
			new object[] { 1, "Alice V2" }, // Should be ignored
            new object[] { 3, "Charlie" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 3. Verify
		await using (var conn = new NpgsqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT id, name FROM users_ign ORDER BY id";
			using var reader = await cmd.ExecuteReaderAsync();

			Assert.True(await reader.ReadAsync());
			Assert.Equal("Alice", reader["name"]); // Unchanged
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Bob", reader["name"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Charlie", reader["name"]);
		}
	}

	[Fact]
	public async Task Sqlite_Upsert_UpdatesExisting_InsertsNew()
	{
		var dbPath = Path.Combine(Path.GetTempPath(), $"sqlite_upsert_{Guid.NewGuid()}.db");
		var cs = $"Data Source={dbPath}";

		try
		{
			// 1. Setup
			await using (var conn = new SqliteConnection(cs))
			{
				await conn.OpenAsync();
				using var cmd = conn.CreateCommand();
				cmd.CommandText = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)";
				await cmd.ExecuteNonQueryAsync();
				cmd.CommandText = "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')";
				await cmd.ExecuteNonQueryAsync();
			}

			// 2. Write
			// Mock Registry for Sqlite options
			var options = new SqliteWriterOptions { Table = "users", Strategy = SqliteWriteStrategy.Upsert, Key = "id" };
			var registry = new DtPipe.Core.Options.OptionsRegistry();
			registry.Register(options);

			await using var writer = new SqliteDataWriter(cs, options, NullLogger<SqliteDataWriter>.Instance);
			var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
			await writer.InitializeAsync(columns);

			var batch = new List<object?[]> {
				new object[] { 1, "Alice V2" },
				new object[] { 3, "Charlie" }
			};
			await writer.WriteBatchAsync(batch);
			await writer.CompleteAsync();

			// 3. Verify
			await using (var conn = new SqliteConnection(cs))
			{
				await conn.OpenAsync();
				using var cmd = conn.CreateCommand();
				cmd.CommandText = "SELECT id, name FROM users ORDER BY id";
				using var reader = await cmd.ExecuteReaderAsync();

				Assert.True(await reader.ReadAsync());
				Assert.Equal("Alice V2", reader["name"]);
				Assert.True(await reader.ReadAsync());
				Assert.Equal("Bob", reader["name"]);
				Assert.True(await reader.ReadAsync());
				Assert.Equal("Charlie", reader["name"]);
			}
		}
		finally
		{
			if (File.Exists(dbPath)) File.Delete(dbPath);
		}
	}

	[Fact]
	public async Task Oracle_Upsert_UpdatesExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;

		// Use cached/reused image if possible or standard one
		var cs = await DockerHelper.GetOracleConnectionString(async () =>
		{
			var container = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		// 1. Setup
		await using (var conn = new OracleConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "BEGIN EXECUTE IMMEDIATE 'DROP TABLE users'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "CREATE TABLE users (id NUMBER(10) PRIMARY KEY, name VARCHAR2(100))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO users VALUES (1, 'Alice')";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO users VALUES (2, 'Bob')";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Write
		var options = new OracleWriterOptions { Table = "users", Strategy = OracleWriteStrategy.Upsert, Key = "id" };
		await using var writer = new OracleDataWriter(cs, options, Microsoft.Extensions.Logging.Abstractions.NullLogger<OracleDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> {
			new object[] { 1, "Alice V2" },
			new object[] { 3, "Charlie" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 3. Verify
		await using (var conn = new OracleConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT id, name FROM users ORDER BY id";
			using var reader = await cmd.ExecuteReaderAsync();

			Assert.True(await reader.ReadAsync());
			Assert.Equal("Alice V2", reader["name"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Bob", reader["name"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Charlie", reader["name"]);
		}
	}

	[Fact]
	public async Task DuckDB_Upsert_CompositeKey_UpdatesExisting_InsertsNew()
	{
		// 1. Setup Table with Composite Key
		await using var connection = new DuckDBConnection(_connectionString);
		await connection.OpenAsync();
		using var cmd = connection.CreateCommand();
		cmd.CommandText = "CREATE TABLE comp_users (region TEXT, branch TEXT, target INT, PRIMARY KEY(region, branch))";
		await cmd.ExecuteNonQueryAsync();
		cmd.CommandText = "INSERT INTO comp_users VALUES ('EU', 'Paris', 100), ('EU', 'Berlin', 200)";
		await cmd.ExecuteNonQueryAsync();

		// 2. Write
		var options = new DuckDbWriterOptions { Table = "comp_users", Strategy = DuckDbWriteStrategy.Upsert, Key = "region,branch" };
		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			new("region", typeof(string), true),
			new("branch", typeof(string), true),
			new("target", typeof(int), false)
		};
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> {
			new object[] { "EU", "Paris", 150 }, // Update
            new object[] { "EU", "Madrid", 300 }  // Insert
        };
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 3. Verify
		var result = await QueryAll("SELECT region, branch, target FROM comp_users ORDER BY region, branch");
		result.Should().HaveCount(3);
		// EU Berlin 200 (Unchanged)
		result[0][1].Should().Be("Berlin"); result[0][2].Should().Be(200);
		// EU Madrid 300 (New)
		result[1][1].Should().Be("Madrid"); result[1][2].Should().Be(300);
		// EU Paris 150 (Updated)
		result[2][1].Should().Be("Paris"); result[2][2].Should().Be(150);
	}

	[Fact]
	public async Task SqlServer_Upsert_CompositeKey_UpdatesExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;
		var cs = await DockerHelper.GetSqlServerConnectionString(async () =>
		{
			var container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		await using (var conn = new SqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "IF OBJECT_ID('CompUsers', 'U') IS NOT NULL DROP TABLE CompUsers; CREATE TABLE CompUsers (Region NVARCHAR(50), Branch NVARCHAR(50), Target INT, PRIMARY KEY(Region, Branch))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO CompUsers VALUES ('EU', 'Paris', 100), ('EU', 'Berlin', 200)";
			await cmd.ExecuteNonQueryAsync();
		}

		var options = new SqlServerWriterOptions { Table = "CompUsers", Strategy = SqlServerWriteStrategy.Upsert, Key = "Region,Branch" };
		await using var writer = new SqlServerDataWriter(cs, options, NullLogger<SqlServerDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			 new("Region", typeof(string), true), new("Branch", typeof(string), true), new("Target", typeof(int), false)
		};
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> { new object[] { "EU", "Paris", 150 }, new object[] { "EU", "Madrid", 300 } };
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		await using (var conn = new SqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT Target FROM CompUsers WHERE Region='EU' AND Branch='Paris'";
			Assert.Equal(150, (int)(await cmd.ExecuteScalarAsync())!); // Update Verified
			cmd.CommandText = "SELECT Target FROM CompUsers WHERE Region='EU' AND Branch='Madrid'";
			Assert.Equal(300, (int)(await cmd.ExecuteScalarAsync())!); // Insert Verified
		}
	}

	[Fact]
	public async Task PostgreSql_Upsert_CompositeKey_UpdatesExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;
		var cs = await DockerHelper.GetPostgreSqlConnectionString(async () =>
		{
			var container = new PostgreSqlBuilder("postgres:15-alpine").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		await using (var conn = new NpgsqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "DROP TABLE IF EXISTS comp_users; CREATE TABLE comp_users (region TEXT, branch TEXT, target INT, PRIMARY KEY(region, branch))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO comp_users VALUES ('EU', 'Paris', 100), ('EU', 'Berlin', 200)";
			await cmd.ExecuteNonQueryAsync();
		}

		var options = new PostgreSqlWriterOptions { Table = "comp_users", Strategy = PostgreSqlWriteStrategy.Upsert, Key = "region,branch" };
		await using var writer = new PostgreSqlDataWriter(cs, options, NullLogger<PostgreSqlDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			 new("region", typeof(string), true), new("branch", typeof(string), true), new("target", typeof(int), false)
		};
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> { new object[] { "EU", "Paris", 150 }, new object[] { "EU", "Madrid", 300 } };
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		await using (var conn = new NpgsqlConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT target FROM comp_users WHERE region='EU' AND branch='Paris'";
			Assert.Equal(150, (int)(await cmd.ExecuteScalarAsync())!);
			cmd.CommandText = "SELECT target FROM comp_users WHERE region='EU' AND branch='Madrid'";
			Assert.Equal(300, (int)(await cmd.ExecuteScalarAsync())!);
		}
	}

	[Fact]
	public async Task Oracle_Upsert_CompositeKey_UpdatesExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;
		var cs = await DockerHelper.GetOracleConnectionString(async () =>
		{
			var container = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		await using (var conn = new OracleConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "BEGIN EXECUTE IMMEDIATE 'DROP TABLE COMP_USERS'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "CREATE TABLE COMP_USERS (REGION VARCHAR2(50), BRANCH VARCHAR2(50), TARGET NUMBER(10), PRIMARY KEY(REGION, BRANCH))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO COMP_USERS VALUES ('EU', 'Paris', 100)";
			await cmd.ExecuteNonQueryAsync();
		}

		var options = new OracleWriterOptions { Table = "COMP_USERS", Strategy = OracleWriteStrategy.Upsert, Key = "REGION,BRANCH" };
		await using var writer = new OracleDataWriter(cs, options, NullLogger<OracleDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			 new("REGION", typeof(string), true), new("BRANCH", typeof(string), true), new("TARGET", typeof(int), false)
		};
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> { new object[] { "EU", "Paris", 150 }, new object[] { "EU", "Madrid", 300 } };
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		await using (var conn = new OracleConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT TARGET FROM COMP_USERS WHERE REGION='EU' AND BRANCH='Paris'";
			var val = await cmd.ExecuteScalarAsync();
			Assert.Equal(150, Convert.ToInt32(val));
			cmd.CommandText = "SELECT TARGET FROM COMP_USERS WHERE REGION='EU' AND BRANCH='Madrid'";
			val = await cmd.ExecuteScalarAsync();
			Assert.Equal(300, Convert.ToInt32(val));
		}
	}

	[Fact]
	public async Task Sqlite_Upsert_CompositeKey_UpdatesExisting_InsertsNew()
	{
		var dbPath = Path.Combine(Path.GetTempPath(), $"sqlite_comp_{Guid.NewGuid()}.db");
		var cs = $"Data Source={dbPath}";

		try
		{
			await using (var conn = new SqliteConnection(cs))
			{
				await conn.OpenAsync();
				using var cmd = conn.CreateCommand();
				cmd.CommandText = "CREATE TABLE comp_users (region TEXT, branch TEXT, target INT, PRIMARY KEY(region, branch))";
				await cmd.ExecuteNonQueryAsync();
				cmd.CommandText = "INSERT INTO comp_users VALUES ('EU', 'Paris', 100)";
				await cmd.ExecuteNonQueryAsync();
			}

			var options = new SqliteWriterOptions { Table = "comp_users", Strategy = SqliteWriteStrategy.Upsert, Key = "region,branch" };
			var registry = new DtPipe.Core.Options.OptionsRegistry();
			registry.Register(options);

			await using var writer = new SqliteDataWriter(cs, options, NullLogger<SqliteDataWriter>.Instance);
			var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
				new("region", typeof(string), true), new("branch", typeof(string), true), new("target", typeof(int), false)
			};
			await writer.InitializeAsync(columns);

			var batch = new List<object?[]> { new object[] { "EU", "Paris", 150 }, new object[] { "EU", "Madrid", 300 } };
			await writer.WriteBatchAsync(batch);
			await writer.CompleteAsync();

			await using (var conn = new SqliteConnection(cs))
			{
				await conn.OpenAsync();
				using var cmd = conn.CreateCommand();
				cmd.CommandText = "SELECT target FROM comp_users WHERE region='EU' AND branch='Paris'";
				Assert.Equal(150, Convert.ToInt32(await cmd.ExecuteScalarAsync()));
				cmd.CommandText = "SELECT target FROM comp_users WHERE region='EU' AND branch='Madrid'";
				Assert.Equal(300, Convert.ToInt32(await cmd.ExecuteScalarAsync()));
			}
		}
		finally
		{
			if (File.Exists(dbPath)) File.Delete(dbPath);
		}
	}


	[Fact]
	public async Task Sqlite_Ignore_SkipsExisting_InsertsNew()
	{
		var dbPath = Path.Combine(Path.GetTempPath(), $"sqlite_ignore_{Guid.NewGuid()}.db");
		var cs = $"Data Source={dbPath}";

		try
		{
			// 1. Setup
			await using (var conn = new SqliteConnection(cs))
			{
				await conn.OpenAsync();
				using var cmd = conn.CreateCommand();
				cmd.CommandText = "CREATE TABLE users_ign (id INT PRIMARY KEY, name TEXT)";
				await cmd.ExecuteNonQueryAsync();
				cmd.CommandText = "INSERT INTO users_ign VALUES (1, 'Alice'), (2, 'Bob')";
				await cmd.ExecuteNonQueryAsync();
			}

			// 2. Write
			var options = new SqliteWriterOptions { Table = "users_ign", Strategy = SqliteWriteStrategy.Ignore, Key = "id" };
			var registry = new DtPipe.Core.Options.OptionsRegistry();
			registry.Register(options);

			await using var writer = new SqliteDataWriter(cs, options, NullLogger<SqliteDataWriter>.Instance);
			var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("id", typeof(int), false), new("name", typeof(string), true) };
			await writer.InitializeAsync(columns);

			var batch = new List<object?[]> {
				new object[] { 1, "Alice V2" }, // Should be ignored
                new object[] { 3, "Charlie" }
			};
			await writer.WriteBatchAsync(batch);
			await writer.CompleteAsync();

			// 3. Verify
			await using (var conn = new SqliteConnection(cs))
			{
				await conn.OpenAsync();
				using var cmd = conn.CreateCommand();
				cmd.CommandText = "SELECT id, name FROM users_ign ORDER BY id";
				using var reader = await cmd.ExecuteReaderAsync();

				Assert.True(await reader.ReadAsync());
				Assert.Equal("Alice", reader["name"]); // Unchanged
				Assert.True(await reader.ReadAsync());
				Assert.Equal("Bob", reader["name"]);
				Assert.True(await reader.ReadAsync());
				Assert.Equal("Charlie", reader["name"]);
			}
		}
		finally
		{
			if (File.Exists(dbPath)) File.Delete(dbPath);
		}
	}

	[Fact]
	public async Task Oracle_Ignore_SkipsExisting_InsertsNew()
	{
		if (!DockerHelper.IsAvailable()) return;

		var cs = await DockerHelper.GetOracleConnectionString(async () =>
		{
			var container = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart").Build();
			await container.StartAsync();
			return container.GetConnectionString();
		});

		// 1. Setup
		await using (var conn = new OracleConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "BEGIN EXECUTE IMMEDIATE 'DROP TABLE USERS_IGN'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "CREATE TABLE USERS_IGN (ID NUMBER(10) PRIMARY KEY, NAME VARCHAR2(100))";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO USERS_IGN VALUES (1, 'Alice')";
			await cmd.ExecuteNonQueryAsync();
			cmd.CommandText = "INSERT INTO USERS_IGN VALUES (2, 'Bob')";
			await cmd.ExecuteNonQueryAsync();
		}

		// 2. Write
		var options = new OracleWriterOptions { Table = "USERS_IGN", Strategy = OracleWriteStrategy.Ignore, Key = "ID" };
		await using var writer = new OracleDataWriter(cs, options, NullLogger<OracleDataWriter>.Instance);
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> { new("ID", typeof(int), false), new("NAME", typeof(string), true) };
		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> {
			new object[] { 1, "Alice V2" }, // Should be ignored
            new object[] { 3, "Charlie" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 3. Verify
		await using (var conn = new OracleConnection(cs))
		{
			await conn.OpenAsync();
			using var cmd = conn.CreateCommand();
			cmd.CommandText = "SELECT ID, NAME FROM USERS_IGN ORDER BY ID";
			using var reader = await cmd.ExecuteReaderAsync();

			Assert.True(await reader.ReadAsync());
			Assert.Equal("Alice", reader["NAME"]); // Unchanged
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Bob", reader["NAME"]);
			Assert.True(await reader.ReadAsync());
			Assert.Equal("Charlie", reader["NAME"]);
		}
	}
}
