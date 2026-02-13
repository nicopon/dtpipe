using DtPipe.Adapters.Oracle;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using Oracle.ManagedDataAccess.Client;
using Testcontainers.Oracle;
using Xunit;

namespace DtPipe.Tests;

[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class OracleSchemaInspectorTests : IAsyncLifetime
{
	private OracleContainer? _oracle;

	public async ValueTask InitializeAsync()
	{
		if (!DockerHelper.IsAvailable()) return;

		try
		{
			_oracle = new OracleBuilder("gvenzl/oracle-xe:21-slim-faststart")
				.Build();
			await _oracle.StartAsync();
		}
		catch (Exception)
		{
			_oracle = null;
		}
	}

	public async ValueTask DisposeAsync()
	{
		if (_oracle is not null)
		{
			await _oracle.DisposeAsync();
		}
	}

	private string GetConnectionString()
	{
		return _oracle!.GetConnectionString();
	}

	private async Task ExecuteSql(string sql)
	{
		await using var connection = new OracleConnection(GetConnectionString());
		await connection.OpenAsync();
		await using var cmd = connection.CreateCommand();
		cmd.CommandText = sql;
		await cmd.ExecuteNonQueryAsync();
	}

	#region Schema Inspection Tests

	[Fact]
	public async Task Oracle_InspectTargetAsync_WhenTableDoesNotExist_ReturnsNotExists()
	{
		if (!DockerHelper.IsAvailable() || _oracle is null) return;

		var options = new OracleWriterOptions { Table = "NON_EXISTENT" };

		await using var writer = new OracleDataWriter(GetConnectionString(), options, NullLogger<OracleDataWriter>.Instance, OracleTypeConverter.Instance);
		var inspector = writer as ISchemaInspector;

		Assert.NotNull(inspector);

		var result = await inspector.InspectTargetAsync();

		Assert.NotNull(result);
		Assert.False(result.Exists);
		Assert.Empty(result.Columns);
	}

	[Fact]
	public async Task Oracle_InspectTargetAsync_DetectsColumnsAndTypes()
	{
		if (!DockerHelper.IsAvailable() || _oracle is null) return;

		// Oracle creates tables in UPPERCASE by default unless quoted
		await ExecuteSql(@"
            CREATE TABLE TYPE_TEST (
                ID NUMBER(10) PRIMARY KEY,
                NAME VARCHAR2(100) NOT NULL,
                VALUE NUMBER(10,2),
                CREATED DATE
            )");

		var options = new OracleWriterOptions { Table = "TYPE_TEST" };

		await using var writer = new OracleDataWriter(GetConnectionString(), options, NullLogger<OracleDataWriter>.Instance, OracleTypeConverter.Instance);
		var inspector = (ISchemaInspector)writer;

		var result = await inspector.InspectTargetAsync();

		Assert.NotNull(result);
		Assert.True(result.Exists);
		Assert.Equal(4, result.Columns.Count);

		// Oracle returns column names in uppercase usually
		var idCol = result.Columns.First(c => c.Name == "ID");
		Assert.True(idCol.IsPrimaryKey);
		Assert.Equal(typeof(decimal), idCol.InferredClrType); // NUMBER usually maps to decimal

		var nameCol = result.Columns.First(c => c.Name == "NAME");
		Assert.False(nameCol.IsNullable);
		Assert.Equal(typeof(string), nameCol.InferredClrType);
		Assert.Equal(100, nameCol.MaxLength);

		var valueCol = result.Columns.First(c => c.Name == "VALUE");
		Assert.Equal(typeof(decimal), valueCol.InferredClrType);

		var dateCol = result.Columns.First(c => c.Name == "CREATED");
		Assert.True(dateCol.IsNullable);
		Assert.Equal(typeof(DateTime), dateCol.InferredClrType);
	}

	#endregion

	#region Schema Compatibility Tests

	[Fact]
	public async Task Oracle_SchemaCompatibility_CompatibleSchema()
	{
		if (!DockerHelper.IsAvailable() || _oracle is null) return;

		await ExecuteSql("CREATE TABLE COMPAT_TEST (ID NUMBER(10), NAME VARCHAR2(50))");

		var sourceSchema = new List<PipeColumnInfo>
		{
			new("ID", typeof(decimal), false), // Match number
            new("NAME", typeof(string), true)
		};

		var options = new OracleWriterOptions { Table = "COMPAT_TEST" };

		await using var writer = new OracleDataWriter(GetConnectionString(), options, NullLogger<OracleDataWriter>.Instance, OracleTypeConverter.Instance);
		var inspector = (ISchemaInspector)writer;

		var targetSchema = await inspector.InspectTargetAsync();
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		Assert.True(report.IsCompatible);
		Assert.Empty(report.Errors);
	}

	[Fact]
	public async Task Oracle_SchemaCompatibility_MissingColumn()
	{
		if (!DockerHelper.IsAvailable() || _oracle is null) return;

		await ExecuteSql("CREATE TABLE MISSING_TEST (ID NUMBER(10))");

		var sourceSchema = new List<PipeColumnInfo>
		{
			new("ID", typeof(decimal), false),
			new("EXTRA_COLUMN", typeof(string), true) // Not in target
        };

		var options = new OracleWriterOptions { Table = "MISSING_TEST" };

		await using var writer = new OracleDataWriter(GetConnectionString(), options, NullLogger<OracleDataWriter>.Instance, OracleTypeConverter.Instance);
		var inspector = (ISchemaInspector)writer;

		var targetSchema = await inspector.InspectTargetAsync();
		var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);

		Assert.False(report.IsCompatible);
		Assert.Contains(report.Columns, c =>
			c.ColumnName == "EXTRA_COLUMN" && c.Status == CompatibilityStatus.MissingInTarget);
	}

	#endregion
}
