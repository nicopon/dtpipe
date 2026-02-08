using DtPipe.Core.Validation;
using Testcontainers.PostgreSql;
using Xunit;
using Npgsql;
using DtPipe.Adapters.PostgreSQL;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli.Abstractions;
using DtPipe.Tests.Helpers;

namespace DtPipe.Tests;

[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class SchemaInspectorIntegrationTests : IAsyncLifetime
{
    private PostgreSqlContainer? _postgres;

    public async ValueTask InitializeAsync()
    {
        if (!DockerHelper.IsAvailable()) return;

        try
        {
            _postgres = new PostgreSqlBuilder("postgres:15-alpine")
                .Build();
            await _postgres.StartAsync();
        }
        catch (Exception)
        {
            _postgres = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_postgres is not null)
        {
            await _postgres.DisposeAsync();
        }
    }

    #region PostgreSQL Schema Inspection Tests

    [Fact]
    public async Task PostgreSql_InspectTargetAsync_WhenTableDoesNotExist_ReturnsNotExists()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        var options = new PostgreSqlWriterOptions { Table = "non_existent_table" };
        
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = writer as ISchemaInspector;
        
        Assert.NotNull(inspector);
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.False(result.Exists);
        Assert.Empty(result.Columns);
    }

    [Fact]
    public async Task PostgreSql_InspectTargetAsync_DetectsColumnsAndTypes()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        // Create a table with various types
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE schema_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email TEXT,
                age INTEGER,
                salary NUMERIC(10,2),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP
            )";
        await cmd.ExecuteNonQueryAsync();
        
        var options = new PostgreSqlWriterOptions { Table = "schema_test" };
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = (ISchemaInspector)writer;
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.True(result.Exists);
        Assert.Equal(7, result.Columns.Count);
        
        // Check column detection
        var idCol = result.Columns.First(c => c.Name == "id");
        Assert.True(idCol.IsPrimaryKey);
        Assert.False(idCol.IsNullable);
        Assert.Equal(typeof(int), idCol.InferredClrType);
        
        var nameCol = result.Columns.First(c => c.Name == "name");
        Assert.False(nameCol.IsNullable);
        Assert.Equal(100, nameCol.MaxLength);
        Assert.Contains("varchar", nameCol.NativeType.ToLower());
        
        var emailCol = result.Columns.First(c => c.Name == "email");
        Assert.True(emailCol.IsNullable);
        Assert.Equal(typeof(string), emailCol.InferredClrType);
        
        var salaryCol = result.Columns.First(c => c.Name == "salary");
        Assert.Equal(typeof(decimal), salaryCol.InferredClrType);
        
        var isActiveCol = result.Columns.First(c => c.Name == "is_active");
        Assert.Equal(typeof(bool), isActiveCol.InferredClrType);
    }

    [Fact]
    public async Task PostgreSql_InspectTargetAsync_DetectsPrimaryKeyAndUnique()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE constraint_test (
                id INTEGER PRIMARY KEY,
                code VARCHAR(20) UNIQUE NOT NULL,
                name TEXT
            )";
        await cmd.ExecuteNonQueryAsync();
        
        var options = new PostgreSqlWriterOptions { Table = "constraint_test" };
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = (ISchemaInspector)writer;
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.NotNull(result.PrimaryKeyColumns);
        Assert.Contains("id", result.PrimaryKeyColumns);
        
        var idCol = result.Columns.First(c => c.Name == "id");
        Assert.True(idCol.IsPrimaryKey);
        
        var codeCol = result.Columns.First(c => c.Name == "code");
        Assert.True(codeCol.IsUnique);
        Assert.False(codeCol.IsNullable);
    }

    [Fact]
    public async Task PostgreSql_InspectTargetAsync_ReturnsRowCountAndSize()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        
        // Create and populate table
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE stats_test (id SERIAL PRIMARY KEY, data TEXT);
            INSERT INTO stats_test (data) VALUES ('row1'), ('row2'), ('row3');
            ANALYZE stats_test;"; // Force stats update
        await cmd.ExecuteNonQueryAsync();
        
        var options = new PostgreSqlWriterOptions { Table = "stats_test" };
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = (ISchemaInspector)writer;
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.True(result.Exists);
        // Row count might be estimate from pg_class, but should be close to 3
        Assert.NotNull(result.RowCount);
        Assert.True(result.RowCount >= 0);
        // Size should be available
        Assert.NotNull(result.SizeBytes);
        Assert.True(result.SizeBytes > 0);
    }

    #endregion

    #region Schema Compatibility Analyzer Integration Tests

    [Fact]
    public async Task SchemaCompatibilityAnalyzer_DetectsCompatibleSchema()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE compat_test (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                value NUMERIC(10,2)
            )";
        await cmd.ExecuteNonQueryAsync();
        
        // Source schema that matches target
        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true),
            new("value", typeof(decimal), true)
        };
        
        var options = new PostgreSqlWriterOptions { Table = "compat_test" };
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        Assert.True(report.IsCompatible);
        Assert.Empty(report.Errors);
        Assert.All(report.Columns, c => 
            Assert.True(c.Status == CompatibilityStatus.Compatible));
    }

    [Fact]
    public async Task SchemaCompatibilityAnalyzer_DetectsMissingColumn()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE missing_col_test (id INTEGER PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        
        // Source has extra column not in target
        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true),
            new("email", typeof(string), true) // Not in target!
        };
        
        var options = new PostgreSqlWriterOptions { Table = "missing_col_test" };
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        Assert.False(report.IsCompatible);
        Assert.Contains(report.Errors, e => e.Contains("email"));
        
        var emailCol = report.Columns.First(c => c.ColumnName == "email");
        Assert.Equal(CompatibilityStatus.MissingInTarget, emailCol.Status);
    }

    [Fact]
    public async Task SchemaCompatibilityAnalyzer_DetectsExtraNotNullColumn()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE extra_notnull_test (
                id INTEGER PRIMARY KEY,
                required_field TEXT NOT NULL
            )";
        await cmd.ExecuteNonQueryAsync();
        
        // Source doesn't have required_field - will fail on insert!
        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false)
        };
        
        var options = new PostgreSqlWriterOptions { Table = "extra_notnull_test" };
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        Assert.False(report.IsCompatible);
        
        var extraCol = report.Columns.First(c => c.ColumnName == "required_field");
        Assert.Equal(CompatibilityStatus.ExtraInTargetNotNull, extraCol.Status);
    }

    [Fact]
    public async Task SchemaCompatibilityAnalyzer_DetectsNullabilityConflict()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE nullability_test (
                id INTEGER PRIMARY KEY,
                status TEXT NOT NULL
            )";
        await cmd.ExecuteNonQueryAsync();
        
        // Source has nullable status, but target requires NOT NULL
        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("status", typeof(string), true) // Nullable in source
        };
        
        var options = new PostgreSqlWriterOptions { Table = "nullability_test" };
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        // Nullability conflict is a warning, not error
        Assert.True(report.IsCompatible);
        
        var statusCol = report.Columns.First(c => c.ColumnName == "status");
        Assert.Equal(CompatibilityStatus.NullabilityConflict, statusCol.Status);
    }

    [Fact]
    public async Task SchemaCompatibilityAnalyzer_WhenTableHasData_AddsWarning()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE existing_data_test (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO existing_data_test VALUES (1, 'Existing'), (2, 'Data');
            ANALYZE existing_data_test;";
        await cmd.ExecuteNonQueryAsync();
        
        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };
        
        var options = new PostgreSqlWriterOptions { Table = "existing_data_test" };
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        // Should have warning about existing data
        Assert.Contains(report.Warnings, w => w.Contains("rows"));
    }

    #endregion
}
