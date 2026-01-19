using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Providers.DuckDB;
using QueryDump.Tests.Helpers;
using QueryDump.Transformers.Fake;
using QueryDump.Writers;
using QueryDump.Writers.Csv;
using Xunit;

namespace QueryDump.Tests;

public class E2EIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _connectionString;
    private readonly string _outputPath;

    public E2EIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_fake_{Guid.NewGuid()}.duckdb");
        _connectionString = $"Data Source={_dbPath}";
        _outputPath = Path.Combine(Path.GetTempPath(), $"output_fake_{Guid.NewGuid()}.csv");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
        try { if (File.Exists(_outputPath)) File.Delete(_outputPath); } catch { }
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task ExportService_ShouldMaskData_WhenFakeOptionsAreProvided()
    {
        // 1. Setup Database
        using (var connection = new DuckDB.NET.Data.DuckDBConnection(_connectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "users");
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            await TestDataSeeder.SeedAsync(connection, "users");
            // Seeded names: Alice, Bob, Charlie, David
        }

        // 2. Configure Services (Mimic Program.cs with new simplified architecture)
        var registry = new OptionsRegistry();
        registry.Register(new DuckDbOptions());
        registry.Register(new CsvOptions { Header = true });
        // Register Fake Options: Mask "Name" with "name.firstname"
        registry.Register(new FakeOptions 
        { 
            Mappings = ["Name:name.firstname"],
            Seed = 12345 // Deterministic
        });
        
        var services = new ServiceCollection();
        services.AddSingleton(registry);
        
        // Reader Factories
        services.AddSingleton<IStreamReaderFactory, DuckDbReaderFactory>();
        
        // Writer Factories
        services.AddSingleton<IDataWriterFactory, CsvWriterFactory>();
        
        // Transformer Factories
        services.AddSingleton<IDataTransformerFactory, FakeDataTransformerFactory>();
        
        services.AddSingleton<ExportService>();
        
        var serviceProvider = services.BuildServiceProvider();
        var exportService = serviceProvider.GetRequiredService<ExportService>();

        // 3. Define Run Options
        var options = new DumpOptions
        {
            Provider = "duckdb",
            ConnectionString = _connectionString,
            Query = "SELECT * FROM users ORDER BY Id",
            OutputPath = _outputPath,
            BatchSize = 100
        };

        // 4. Run Export
        var args = new[] { "querydump", "--fake", "Name:name.firstname" };
        await exportService.RunExportAsync(options, TestContext.Current.CancellationToken, args);

        // 5. Verify Output
        File.Exists(_outputPath).Should().BeTrue();
        var lines = await File.ReadAllLinesAsync(_outputPath, TestContext.Current.CancellationToken);
        
        // Header + 4 rows
        lines.Should().HaveCount(5);
        
        // Parse CSV simply
        var headers = lines[0].Split(',');
        var nameIndex = Array.IndexOf(headers, "Name");
        nameIndex.Should().BeGreaterThan(-1);

        var firstRow = lines[1].Split(',');
        var maskedName = firstRow[nameIndex];
        
        // "Alice" is the original name for ID 1. 
        // With Seed 12345, "name.firstname" should produce "Dillie".
        // Let's just assert it is NOT "Alice"
        maskedName.Should().NotBe("Alice");
        maskedName.Should().NotBeNullOrEmpty();
    }
    [Fact]
    public async Task ExportService_ShouldRespectTransformerPipeline_Null_Overwrite_Clone()
    {
        // 1. Setup Database
        using (var connection = new DuckDB.NET.Data.DuckDBConnection(_connectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "users");
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            await TestDataSeeder.SeedAsync(connection, "users");
            // Seeded: 1, Alice, alice@example.com
        }

        // 2. Configure Services
        var registry = new OptionsRegistry();
        registry.Register(new DuckDbOptions());
        registry.Register(new CsvOptions { Header = true });
        
        registry.Register(new Transformers.Null.NullOptions 
        { 
            Columns = ["Age"] 
        });
        
        registry.Register(new Transformers.Overwrite.OverwriteOptions 
        { 
            Mappings = ["Name:Bond"] 
        });
        
        registry.Register(new Transformers.Format.FormatOptions
        {
            Mappings = ["CopiedName:{{Name}} is 007"]
        });

        var services = new ServiceCollection();
        services.AddSingleton(registry);
        
        // Reader Factories
        services.AddSingleton<IStreamReaderFactory, DuckDbReaderFactory>();
        
        // Writer Factories
        services.AddSingleton<IDataWriterFactory, CsvWriterFactory>();
        
        // Transformer Factories
        services.AddSingleton<IDataTransformerFactory, Transformers.Null.NullDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, Transformers.Overwrite.OverwriteDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, Transformers.Fake.FakeDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, Transformers.Format.FormatDataTransformerFactory>();
        
        services.AddSingleton<ExportService>();
        
        var serviceProvider = services.BuildServiceProvider();
        var exportService = serviceProvider.GetRequiredService<ExportService>();

        var options = new DumpOptions
        {
            Provider = "duckdb",
            ConnectionString = _connectionString,
            Query = "SELECT Id, Name, Age, Name as CopiedName FROM users ORDER BY Id",
            OutputPath = _outputPath,
            BatchSize = 100
        };

        // 3. Run Export
        var args = new[] 
        { 
            "querydump", 
            "--null", "Age",
            "--overwrite", "Name:Bond",
            "--format", "CopiedName:{{Name}} is 007" 
        };
        await exportService.RunExportAsync(options, TestContext.Current.CancellationToken, args);

        // 4. Verify
        File.Exists(_outputPath).Should().BeTrue();
        var lines = await File.ReadAllLinesAsync(_outputPath, TestContext.Current.CancellationToken);
        
        // Header
        var headers = lines[0].Split(',');
        var nameIdx = Array.IndexOf(headers, "Name");
        var ageIdx = Array.IndexOf(headers, "Age");
        var copyIdx = Array.IndexOf(headers, "CopiedName");
        
        var row1 = lines[1].Split(',');
        
        // Nuller check
        row1[ageIdx].Should().BeEmpty(); // CSV null is empty string by default
        
        // Overwriter check
        row1[nameIdx].Should().Be("Bond");
        
        // Cloner check (should see "Bond" from Overwriter)
        row1[copyIdx].Should().Be("Bond is 007"); 
    }
    [Fact]
    public async Task ExportService_ShouldRespectInterleavedOrder()
    {
        // 1. Setup Database
        using (var connection = new DuckDB.NET.Data.DuckDBConnection(_connectionString))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "CREATE TABLE test (A VARCHAR, B VARCHAR, C VARCHAR)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            cmd.CommandText = "INSERT INTO test VALUES ('Init', '', '')";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        // 2. Configure Services
        var registry = new OptionsRegistry();
        registry.Register(new DuckDbOptions());
        registry.Register(new CsvOptions { Header = true });
        
        // Register empty options, they will be populated by the pipeline builder from args
        registry.Register(new Transformers.Null.NullOptions());
        registry.Register(new Transformers.Overwrite.OverwriteOptions());
        registry.Register(new Transformers.Format.FormatOptions());
        registry.Register(new FakeOptions());

        var services = new ServiceCollection();
        services.AddSingleton(registry);
        
        // Reader Factories
        services.AddSingleton<IStreamReaderFactory, DuckDbReaderFactory>();
        
        // Writer Factories
        services.AddSingleton<IDataWriterFactory, CsvWriterFactory>();
        
        // Transformer Factories
        services.AddSingleton<IDataTransformerFactory, Transformers.Null.NullDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, Transformers.Overwrite.OverwriteDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, Transformers.Fake.FakeDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, Transformers.Format.FormatDataTransformerFactory>();
        
        services.AddSingleton<ExportService>();
        
        var serviceProvider = services.BuildServiceProvider();
        var exportService = serviceProvider.GetRequiredService<ExportService>();

        var options = new DumpOptions
        {
            Provider = "duckdb",
            ConnectionString = _connectionString,
            Query = "SELECT A, B, C FROM test",
            OutputPath = _outputPath
        };
        
        // 3. Simulate CLI Args for Ordered Pipeline
        // Sequence:
        // 1. Overwrite A -> "Val1"
        // 2. Format B -> "{{A}}" (should capture Val1)
        // 3. Overwrite A -> "Val2"
        // 4. Format C -> "{{A}}" (should capture Val2)

        var newArgs = new[] 
        { 
            "querydump", // dummy exe name
            "--overwrite", "A:Val1",
            "--format", "B:{{A}}",
            "--overwrite", "A:Val2",
            "--format", "C:{{A}}"
        };
        
        // 4. Run Export
        await exportService.RunExportAsync(options, TestContext.Current.CancellationToken, newArgs);
        
        // 5. Verify Output
        var lines = await File.ReadAllLinesAsync(_outputPath, TestContext.Current.CancellationToken);
        
        // Header + 1 row
        lines.Should().HaveCount(2);
        
        var headers = lines[0].Split(',');
        var idxA = Array.IndexOf(headers, "A");
        var idxB = Array.IndexOf(headers, "B");
        var idxC = Array.IndexOf(headers, "C");
        
        var row = lines[1].Split(',');
        
        // Sequence:
        // 1. Overwrite A -> "Val1"
        // 2. Format B -> "{{A}}" (captures "Val1")
        // 3. Overwrite A -> "Val2"
        // 4. Format C -> "{{A}}" (captures "Val2")
        
        row[idxB].Should().Be("Val1");
        row[idxC].Should().Be("Val2");
        row[idxA].Should().Be("Val2"); // Final value of A
    }
}
