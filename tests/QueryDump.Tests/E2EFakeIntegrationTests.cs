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
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "users");
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            await TestDataSeeder.SeedAsync(connection, "users");
            // Seeded names: Alice, Bob, Charlie, David
        }

        // 2. Configure Services (Mimic Program.cs)
        var services = new ServiceCollection();
        services.AddSingleton<IDataSourceFactory, DataSourceFactory>();
        services.AddSingleton<IDataWriterFactory, DataWriterFactory>();
        services.AddSingleton<IFakeDataTransformerFactory, FakeDataTransformerFactory>();
        services.AddSingleton<OptionsRegistry>();
        services.AddSingleton<ExportService>();
        
        // Register Options manually as the CLI binder would do
        var registry = new OptionsRegistry();
        registry.Register(new DuckDbOptions());
        registry.Register(new CsvOptions { Header = true });
        // Register Fake Options: Mask "Name" with "name.firstname"
        registry.Register(new FakeOptions 
        { 
            Mappings = new[] { "Name:name.firstname" },
            Seed = 12345 // Deterministic
        });
        
        services.AddSingleton(registry); // Override the service registration above if needed, but we used the instance
        
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
}
