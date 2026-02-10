using DtPipe.Adapters.DuckDB;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests;

public class DuckDbSchemaInspectorTests : IDisposable
{
	private readonly string _dbPath;
	private readonly string _connectionString;

	public DuckDbSchemaInspectorTests()
	{
		_dbPath = Path.Combine(Path.GetTempPath(), $"dtpipe_test_{Guid.NewGuid()}.db");
		_connectionString = $"Data Source={_dbPath}";
	}

	public void Dispose()
	{
		if (File.Exists(_dbPath))
		{
			try
			{
				File.Delete(_dbPath);
			}
			catch
			{
				// Ignore cleanup errors
			}
		}
	}

	private async Task ExecuteSql(string sql)
	{
		await using var connection = new DuckDBConnection(_connectionString);
		await connection.OpenAsync();
		await using var cmd = connection.CreateCommand();
		cmd.CommandText = sql;
		await cmd.ExecuteNonQueryAsync();
	}

	#region Schema Inspection Tests

	[Fact]
	public async Task DuckDb_InspectTargetAsync_WhenTableDoesNotExist_ReturnsNotExists()
	{
		var options = new DuckDbWriterOptions { Table = "non_existent" };

		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);
		var inspector = writer as ISchemaInspector;

		Assert.NotNull(inspector);

		var result = await inspector.InspectTargetAsync();

		Assert.NotNull(result);
		Assert.False(result.Exists);
		Assert.Empty(result.Columns);
	}

	[Fact]
	public async Task DuckDb_InspectTargetAsync_DetectsColumnsAndTypes()
	{
		await ExecuteSql(@"
            CREATE TABLE type_test (
                id INTEGER,
                name VARCHAR,
                age INTEGER,
                salary DECIMAL(10,2),
                is_active BOOLEAN
            )");

		var options = new DuckDbWriterOptions { Table = "type_test" };

		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);
		var inspector = (ISchemaInspector)writer;

		var result = await inspector.InspectTargetAsync();

		Assert.NotNull(result);
		Assert.True(result.Exists);
		Assert.Equal(5, result.Columns.Count);

		var idCol = result.Columns.First(c => c.Name == "id");
		Assert.Equal(typeof(int), idCol.InferredClrType);

		var nameCol = result.Columns.First(c => c.Name == "name");
		Assert.Equal(typeof(string), nameCol.InferredClrType);

		var salaryCol = result.Columns.First(c => c.Name == "salary");
		Assert.True(salaryCol.InferredClrType == typeof(decimal) || salaryCol.InferredClrType == typeof(double));

		var isActiveCol = result.Columns.First(c => c.Name == "is_active");
		Assert.Equal(typeof(bool), isActiveCol.InferredClrType);
	}

	[Fact]
	public async Task DuckDb_InspectTargetAsync_ReturnsRowCount()
	{
		await ExecuteSql(@"
            CREATE TABLE count_test (id INTEGER);
            INSERT INTO count_test VALUES (1), (2), (3);");

		var options = new DuckDbWriterOptions { Table = "count_test" };

		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);
		var inspector = (ISchemaInspector)writer;

		var result = await inspector.InspectTargetAsync();

		Assert.NotNull(result);
		Assert.True(result.Exists);
		Assert.NotNull(result.RowCount);
		Assert.Equal(3, result.RowCount);
	}

	[Fact]
	public async Task DuckDb_InspectTargetAsync_DetectsNotNull()
	{
		await ExecuteSql(@"
            CREATE TABLE notnull_test (
                id INTEGER NOT NULL,
                name VARCHAR
            )");

		var options = new DuckDbWriterOptions { Table = "notnull_test" };

		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);
		var inspector = (ISchemaInspector)writer;

		var result = await inspector.InspectTargetAsync();

		var idCol = result!.Columns.First(c => c.Name == "id");
		Assert.False(idCol.IsNullable);

		var nameCol = result.Columns.First(c => c.Name == "name");
		Assert.True(nameCol.IsNullable);
	}

	#endregion

	#region Schema Compatibility Tests

	[Fact]
	public async Task DuckDb_SchemaCompatibility_CompatibleSchema()
	{
		await ExecuteSql("CREATE TABLE compat_test (id INTEGER, name VARCHAR)");

		var sourceSchema = new List<PipeColumnInfo>
		{
			new("id", typeof(int), false),
			new("name", typeof(string), true)
		};

		var options = new DuckDbWriterOptions { Table = "compat_test" };

		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);
		var inspector = (ISchemaInspector)writer;

		var targetSchema = await inspector.InspectTargetAsync();
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		Assert.True(report.IsCompatible);
		Assert.Empty(report.Errors);
	}

	[Fact]
	public async Task DuckDb_SchemaCompatibility_MissingColumn()
	{
		await ExecuteSql("CREATE TABLE missing_test (id INTEGER)");

		var sourceSchema = new List<PipeColumnInfo>
		{
			new("id", typeof(int), false),
			new("extra_column", typeof(string), true)
		};

		var options = new DuckDbWriterOptions { Table = "missing_test" };

		await using var writer = new DuckDbDataWriter(_connectionString, options, NullLogger<DuckDbDataWriter>.Instance);
		var inspector = (ISchemaInspector)writer;

		var targetSchema = await inspector.InspectTargetAsync();
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		Assert.False(report.IsCompatible);
		Assert.Contains(report.Columns, c =>
			c.ColumnName == "extra_column" && c.Status == CompatibilityStatus.MissingInTarget);
	}

	#endregion
}
