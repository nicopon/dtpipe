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

public class E2EFakeIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _connectionString;
    private readonly string _outputPath;

    public E2EFakeIntegrationTests()
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
            Mappings = new[] { "Name:name.firstname" },
            Seed = 12345 // Deterministic
        });
        
        var services = new ServiceCollection();
        services.AddSingleton(registry);
        
        // Reader Factories
        services.AddSingleton<IReaderFactory, DuckDbReaderFactory>();
        
        // Writer Factories
        services.AddSingleton<IWriterFactory, CsvWriterFactory>();
        
        // Transformer Factories
        services.AddSingleton<ITransformerFactory, FakeDataTransformerFactory>();
        
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
        await exportService.RunExportAsync(options, TestContext.Current.CancellationToken);

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
            Columns = new[] { "Age" } 
        });
        
        registry.Register(new Transformers.Static.OverwriteOptions 
        { 
            Mappings = new[] { "Name:Bond" } 
        });
        
        registry.Register(new Transformers.Clone.CloneOptions
        {
            Mappings = new[] { "CopiedName:{{Name}} is 007" }
        });

        var services = new ServiceCollection();
        services.AddSingleton(registry);
        
        // Reader Factories
        services.AddSingleton<IReaderFactory, DuckDbReaderFactory>();
        
        // Writer Factories
        services.AddSingleton<IWriterFactory, CsvWriterFactory>();
        
        // Transformer Factories
        services.AddSingleton<ITransformerFactory, Transformers.Null.NullDataTransformerFactory>();
        services.AddSingleton<ITransformerFactory, Transformers.Static.StaticDataTransformerFactory>();
        services.AddSingleton<ITransformerFactory, Transformers.Fake.FakeDataTransformerFactory>();
        services.AddSingleton<ITransformerFactory, Transformers.Clone.CloneDataTransformerFactory>();
        
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
        await exportService.RunExportAsync(options, TestContext.Current.CancellationToken);

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
}
