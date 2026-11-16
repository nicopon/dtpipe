using System.Text.Json;
using AwesomeAssertions;
using DtPipe.Cli;
using DtPipe.Cli.Mcp;
using DtPipe.Sessions;
using DtPipe.Tests.Helpers;
using DuckDB.NET.Data;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// A sample refuses a source that could mutate before the source has run anything. The verdict
/// reads the SQL the options carry, so nothing about it waits for the reader to open; a refusal
/// that came after the open would report a destruction it had already let happen.
/// </summary>
[Collection(SessionStateCollection.Name)]
public sealed class SampleModeRefusalOrderTests : IDisposable
{
    private readonly string _tmp;
    private readonly string _cwd;
    private readonly string? _savedState;

    public SampleModeRefusalOrderTests()
    {
        _cwd = Directory.GetCurrentDirectory();
        _tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_refusalorder_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tmp);
        _savedState = Environment.GetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable);
        Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, Path.Combine(_tmp, "state"));
        Directory.SetCurrentDirectory(_tmp);
    }

    public void Dispose()
    {
        Directory.SetCurrentDirectory(_cwd);
        Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, _savedState);
        try { Directory.Delete(_tmp, recursive: true); } catch { /* best effort */ }
    }

    private static DtPipeMcpTools McpTools()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
        }));
        return services.BuildServiceProvider().GetRequiredService<DtPipeMcpTools>();
    }

    private static long Scalar(string database, string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={database}");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }

    // A file that survives the process: an in-memory database holds nothing to destroy.
    private const string Database = "victim.duckdb";

    private static void SeedVictim()
    {
        using var seed = new DuckDBConnection($"Data Source={Database}");
        seed.Open();
        using var create = seed.CreateCommand();
        create.CommandText = "CREATE TABLE victim AS SELECT range AS n FROM range(3)";
        create.ExecuteNonQuery();
    }

    private static async Task ShouldRefuse(string job)
    {
        var json = await McpTools().DryRun(job, rows: 1);
        var reply = JsonDocument.Parse(json).RootElement;

        reply.GetProperty("success").GetBoolean().Should().BeFalse(json);
        reply.GetProperty("error").GetString().Should().Contain("Sample mode refuses", json);
    }

    private static void VictimShouldStand()
    {
        Scalar(Database, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'victim'")
            .Should().Be(1, "the refusal came after the source dropped the table it was refusing to drop");
        Scalar(Database, "SELECT count(*) FROM victim").Should().Be(3);
    }

    [Theory]
    [InlineData("DROP TABLE victim")]
    [InlineData("'@drop.sql'")]
    public async Task A_Destructive_Init_Is_Refused_Before_The_Source_Has_Run_It(string initSql)
    {
        SeedVictim();
        // A reference is judged by the text it designates, which the reader reads only at its open.
        File.WriteAllText("drop.sql", "DROP TABLE victim");

        await ShouldRefuse($"""
            main:
              input: duck:{Database}
              provider-options:
                duck:
                  query: SELECT 1 AS x
                  init-sql: {initSql}
              output: csv:out.csv
            """);

        VictimShouldStand();
    }

    [Fact]
    public async Task A_Processors_Init_Is_Refused_Before_The_Processor_Has_Run_It()
    {
        // The processor's SQL lives in its own block, never in a reader's options.
        await ShouldRefuse("""
            a:
              input: generate:3
            b:
              from: a
              provider-options:
                sql:
                  query: SELECT * FROM a
                  duck-init: ATTACH 'attached.db' AS y
              output: csv:out.csv
            """);

        File.Exists("attached.db").Should().BeFalse("the processor attached a file the sample was refusing to attach");
    }
}
