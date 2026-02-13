using DtPipe.Adapters.DuckDB;
using DuckDB.NET.Data;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests.Integration;

public class CaseSensitivityIntegrationTests : IAsyncLifetime
{
	private readonly string _dbPath;
	private readonly string _connectionString;

	public CaseSensitivityIntegrationTests()
	{
		_dbPath = Path.Combine(Path.GetTempPath(), $"cs_test_{Guid.NewGuid()}.duckdb");
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
	public async Task DuckDB_QuotedIdentifiers_ArePreserved()
	{
		// 1. Setup Table with Quoted Identifiers (mixed case)
		await using var connection = new DuckDBConnection(_connectionString);
		await connection.OpenAsync();
		using var cmd = connection.CreateCommand();
		// DuckDB quotes use double quotes
		cmd.CommandText = "CREATE TABLE \"MixedCaseTable\" (\"Id\" INT PRIMARY KEY, \"MixedCol\" TEXT)";
		await cmd.ExecuteNonQueryAsync();

		// 2. Prepare Writer targeting the table
		// We use "MixedCaseTable" as table name.
		// The Dialect should check if it needs quoting based on options.
		var options = new DuckDbWriterOptions
		{
			Table = "MixedCaseTable",
			Strategy = DuckDbWriteStrategy.Append
		};
		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance, DuckDbTypeConverter.Instance);

		// 3. Define Source Columns
		// If IsCaseSensitive=true, Writer should Quote it.
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			new("Id", typeof(int), false, IsCaseSensitive: true),
			new("MixedCol", typeof(string), true, IsCaseSensitive: true)
		};

		await writer.InitializeAsync(columns);

		// 4. Write Data
		var batch = new List<object?[]> {
			new object[] { 1, "Data" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		// 5. Verify
		// We select using quotes to ensure we hit the right column
		var result = await QueryAll("SELECT \"Id\", \"MixedCol\" FROM \"MixedCaseTable\"");
		result.Should().HaveCount(1);
		result[0][1].Should().Be("Data");
	}

	[Fact]
	public async Task DuckDB_Unquoted_IsInsensitive()
	{
		// 1. Setup Table with simple name (lower)
		await using var connection = new DuckDBConnection(_connectionString);
		await connection.OpenAsync();
		using var cmd = connection.CreateCommand();
		cmd.CommandText = "CREATE TABLE simple_table (id INT, val TEXT)";
		await cmd.ExecuteNonQueryAsync();

		// 2. Write using Mixed Case source names (e.g. from CSV header)
		// But IsCaseSensitive = false (default)
		var options = new DuckDbWriterOptions
		{
			Table = "simple_table",
			Strategy = DuckDbWriteStrategy.Append
		};
		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance, DuckDbTypeConverter.Instance);

		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			new("Id", typeof(int), false, IsCaseSensitive: false), // "Id" -> SafeIdentifier "Id" (unquoted) -> Matches "id" in DuckDB
            new("Val", typeof(string), true, IsCaseSensitive: false)
		};

		await writer.InitializeAsync(columns);

		var batch = new List<object?[]> {
			new object[] { 1, "Test" }
		};
		await writer.WriteBatchAsync(batch);
		await writer.CompleteAsync();

		var result = await QueryAll("SELECT id, val FROM simple_table");
		result.Should().HaveCount(1);
	}

	[Fact]
	public async Task DuckDB_Upsert_WithQuotedKey_ShouldQuoteConflictTarget()
	{
		// 1. Setup Table with Quoted PK
		await using var connection = new DuckDBConnection(_connectionString);
		await connection.OpenAsync();
		using var cmd = connection.CreateCommand();
		cmd.CommandText = "CREATE TABLE \"QuotedTable\" (\"Id\" INT PRIMARY KEY, \"Val\" TEXT)";
		await cmd.ExecuteNonQueryAsync();

		// 2. Prepare Writer
		var options = new DuckDbWriterOptions
		{
			Table = "QuotedTable",
			Strategy = DuckDbWriteStrategy.Upsert,
			Key = "Id" // Passed as string
		};
		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance, DuckDbTypeConverter.Instance);

		// 3. Columns with Case Sensitivity = true
		var columns = new List<DtPipe.Core.Models.PipeColumnInfo> {
			new("Id", typeof(int), false, IsCaseSensitive: true),
			new("Val", typeof(string), true, IsCaseSensitive: true)
		};
		await writer.InitializeAsync(columns);

		// 4. Upsert Batch
		var batch = new List<object?[]> {
			new object[] { 1, "Upserted" }
		};
		await writer.WriteBatchAsync(batch);

		// This fails if ON CONFLICT (Id) is generated instead of ON CONFLICT ("Id")
		await writer.CompleteAsync();

		// 5. Verify
		var result = await QueryAll("SELECT \"Val\" FROM \"QuotedTable\" WHERE \"Id\" = 1");
		result.Should().HaveCount(1);
		result[0][0].Should().Be("Upserted");
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
}
