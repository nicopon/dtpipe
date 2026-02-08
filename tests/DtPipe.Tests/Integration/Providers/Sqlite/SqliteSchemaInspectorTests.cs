using DtPipe.Core.Validation;
using Microsoft.Data.Sqlite;
using DtPipe.Adapters.Sqlite;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Options;
using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// SQLite schema inspection tests - no Docker required
/// </summary>
public class SqliteSchemaInspectorTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _connectionString;
    private readonly OptionsRegistry _registry;

    public SqliteSchemaInspectorTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"dtpipe_test_{Guid.NewGuid()}.db");
        _connectionString = $"Data Source={_dbPath}";
        _registry = new OptionsRegistry();
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
        {
            File.Delete(_dbPath);
        }
    }

    private async Task ExecuteSql(string sql)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    #region Schema Inspection Tests

    [Fact]
    public async Task Sqlite_InspectTargetAsync_WhenTableDoesNotExist_ReturnsNotExists()
    {
        _registry.Register(new SqliteWriterOptions { Table = "non_existent" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = writer as ISchemaInspector;
        
        Assert.NotNull(inspector);
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.False(result.Exists);
        Assert.Empty(result.Columns);
    }

    [Fact]
    public async Task Sqlite_InspectTargetAsync_DetectsColumnsAndTypes()
    {
        await ExecuteSql(@"
            CREATE TABLE type_test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                salary REAL,
                data BLOB
            )");

        _registry.Register(new SqliteWriterOptions { Table = "type_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.True(result.Exists);
        Assert.Equal(5, result.Columns.Count);
        
        var idCol = result.Columns.First(c => c.Name == "id");
        Assert.True(idCol.IsPrimaryKey);
        Assert.Contains("INTEGER", idCol.NativeType);
        
        var nameCol = result.Columns.First(c => c.Name == "name");
        Assert.False(nameCol.IsNullable);
        Assert.Equal(typeof(string), nameCol.InferredClrType);
        
        var ageCol = result.Columns.First(c => c.Name == "age");
        Assert.True(ageCol.IsNullable);
        
        var salaryCol = result.Columns.First(c => c.Name == "salary");
        Assert.Equal(typeof(double), salaryCol.InferredClrType);
        
        var dataCol = result.Columns.First(c => c.Name == "data");
        Assert.Equal(typeof(byte[]), dataCol.InferredClrType);
    }

    [Fact]
    public async Task Sqlite_InspectTargetAsync_DetectsPrimaryKey()
    {
        await ExecuteSql(@"
            CREATE TABLE pk_test (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL
            )");

        _registry.Register(new SqliteWriterOptions { Table = "pk_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.NotNull(result.PrimaryKeyColumns);
        Assert.Contains("user_id", result.PrimaryKeyColumns);
        
        var pkCol = result.Columns.First(c => c.Name == "user_id");
        Assert.True(pkCol.IsPrimaryKey);
    }

    [Fact]
    public async Task Sqlite_InspectTargetAsync_ReturnsRowCount()
    {
        await ExecuteSql(@"
            CREATE TABLE count_test (id INTEGER PRIMARY KEY, value TEXT);
            INSERT INTO count_test VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');");

        _registry.Register(new SqliteWriterOptions { Table = "count_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.True(result.Exists);
        Assert.NotNull(result.RowCount);
        Assert.Equal(5, result.RowCount);
    }

    [Fact]
    public async Task Sqlite_InspectTargetAsync_ReturnsFileSize()
    {
        await ExecuteSql(@"
            CREATE TABLE size_test (id INTEGER PRIMARY KEY, data TEXT);
            INSERT INTO size_test VALUES (1, 'some data here');");

        _registry.Register(new SqliteWriterOptions { Table = "size_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var result = await inspector.InspectTargetAsync();
        
        Assert.NotNull(result);
        Assert.NotNull(result.SizeBytes);
        Assert.True(result.SizeBytes > 0);
    }

    [Fact]
    public async Task Sqlite_InspectTargetAsync_DetectsVarcharLength()
    {
        await ExecuteSql("CREATE TABLE length_test (id INTEGER PRIMARY KEY, code VARCHAR(10), description VARCHAR(500))");

        _registry.Register(new SqliteWriterOptions { Table = "length_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var result = await inspector.InspectTargetAsync();
        
        var codeCol = result!.Columns.First(c => c.Name == "code");
        Assert.Equal(10, codeCol.MaxLength);
        
        var descCol = result.Columns.First(c => c.Name == "description");
        Assert.Equal(500, descCol.MaxLength);
    }

    #endregion

    #region Schema Compatibility Tests

    [Fact]
    public async Task Sqlite_SchemaCompatibility_CompatibleSchema()
    {
        await ExecuteSql("CREATE TABLE compat_test (id INTEGER PRIMARY KEY, name TEXT, value REAL)");

        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true),
            new("value", typeof(double), true)
        };

        _registry.Register(new SqliteWriterOptions { Table = "compat_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        Assert.True(report.IsCompatible);
        Assert.Empty(report.Errors);
    }

    [Fact]
    public async Task Sqlite_SchemaCompatibility_MissingColumn()
    {
        await ExecuteSql("CREATE TABLE missing_test (id INTEGER PRIMARY KEY)");

        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("extra_column", typeof(string), true) // Not in target
        };

        _registry.Register(new SqliteWriterOptions { Table = "missing_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        Assert.False(report.IsCompatible);
        Assert.Contains(report.Columns, c => 
            c.ColumnName == "extra_column" && c.Status == CompatibilityStatus.MissingInTarget);
    }

    [Fact]
    public async Task Sqlite_SchemaCompatibility_ExtraNotNullColumn()
    {
        await ExecuteSql("CREATE TABLE extra_nn_test (id INTEGER PRIMARY KEY, required TEXT NOT NULL)");

        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false)
            // Missing 'required' which is NOT NULL in target
        };

        _registry.Register(new SqliteWriterOptions { Table = "extra_nn_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        Assert.False(report.IsCompatible);
        Assert.Contains(report.Columns, c => 
            c.ColumnName == "required" && c.Status == CompatibilityStatus.ExtraInTargetNotNull);
    }

    [Fact]
    public async Task Sqlite_SchemaCompatibility_NullabilityConflict()
    {
        await ExecuteSql("CREATE TABLE null_test (id INTEGER PRIMARY KEY, status TEXT NOT NULL)");

        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("status", typeof(string), true) // Nullable in source, NOT NULL in target
        };

        _registry.Register(new SqliteWriterOptions { Table = "null_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        var statusCol = report.Columns.First(c => c.ColumnName == "status");
        Assert.Equal(CompatibilityStatus.NullabilityConflict, statusCol.Status);
    }

    [Fact]
    public async Task Sqlite_SchemaCompatibility_ExistingDataWarning()
    {
        await ExecuteSql(@"
            CREATE TABLE data_warn_test (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO data_warn_test VALUES (1, 'Alice'), (2, 'Bob');");

        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };

        _registry.Register(new SqliteWriterOptions { Table = "data_warn_test" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        Assert.Contains(report.Warnings, w => w.Contains("2") && w.Contains("rows"));
    }

    [Fact]
    public async Task Sqlite_SchemaCompatibility_WhenTableNotExists_AllColumnsWillBeCreated()
    {
        var sourceSchema = new List<PipeColumnInfo>
        {
            new("id", typeof(int), false),
            new("name", typeof(string), true),
            new("value", typeof(decimal), true)
        };

        _registry.Register(new SqliteWriterOptions { Table = "will_be_created" });
        
        await using var writer = new SqliteDataWriter(_connectionString, _registry);
        var inspector = (ISchemaInspector)writer;
        
        var targetSchema = await inspector.InspectTargetAsync();
        var report = SchemaCompatibilityAnalyzer.Analyze(sourceSchema, targetSchema);
        
        Assert.True(report.IsCompatible);
        Assert.All(report.Columns, c => Assert.Equal(CompatibilityStatus.WillBeCreated, c.Status));
    }

    #endregion
}
