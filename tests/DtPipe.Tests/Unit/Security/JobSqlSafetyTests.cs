using DtPipe.Cli.Mcp;
using DtPipe.Cli.Studio;
using DtPipe.Tests.Unit.Cli;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Security;

/// <summary>
/// The safety check of <c>execute-yaml-job</c> and of the studio's real run classifies the SQL a job
/// carries, never the YAML around it: a key, a comment or a transformer option is not SQL.
/// </summary>
[Collection("console-serial")]
public sealed class JobSqlSafetyTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("job-sql-safety-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DtPipeMcpTools Mcp()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        return services.BuildServiceProvider().GetRequiredService<DtPipeMcpTools>();
    }

    private static StudioEngine Engine()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        return StudioEngines.Real(services.BuildServiceProvider());
    }

    private const string ProjectDrop =
        "main:\n  input: \"generate:5\"\n  output: \"null:\"\n  transformers:\n    - type: project\n      options:\n        drop: GenerateIndex\n";

    [Fact]
    public async Task A_Project_Drop_Option_Is_Not_A_Destructive_Verb_In_A_Real_Run()
    {
        var outcome = await Engine().RunAsync(ProjectDrop, StudioBindings.None, Ct);

        Assert.Null(outcome.Fault);
        Assert.Equal(0, outcome.ExitCode);
    }

    [Theory]
    [InlineData("project: \"Update,Insert\"")]
    [InlineData("rename: \"Delete:Truncate\"")]
    public async Task A_Column_Named_After_A_Verb_Does_Not_Reach_The_Safety_Stage(string option)
    {
        var yaml = "main:\n  input: \"generate:5\"\n  output: \"null:\"\n  transformers:\n    - type: project\n      options:\n        "
            + option + "\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.DoesNotContain("\"stage\": \"safety\"", json);
    }

    [Fact]
    public async Task A_Verb_In_A_Yaml_Comment_Does_Not_Reach_The_Safety_Stage()
    {
        var yaml = "# drop and delete the old rows first\nmain:\n  input: \"generate:5\"\n  output: \"null:\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.DoesNotContain("\"stage\": \"safety\"", json);
    }

    [Fact]
    public async Task A_Query_File_Holding_A_Destructive_Verb_Is_Refused()
    {
        var file = Path.Combine(_dir, "x.sql");
        await File.WriteAllTextAsync(file, "DROP TABLE sales", Ct);
        var yaml = $"main:\n  input: \"duck::memory:\"\n  output: \"null:\"\n  provider-options:\n    duck:\n      query: \"@{file}\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.Contains("\"stage\": \"safety\"", json);
        Assert.Contains("DROP", json);
    }

    [Fact]
    public async Task A_Query_File_That_Cannot_Be_Read_Stops_The_Job_Before_It_Runs()
    {
        var yaml = $"main:\n  input: \"duck::memory:\"\n  output: \"null:\"\n  provider-options:\n    duck:\n      query: \"@{Path.Combine(_dir, "missing.sql")}\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.Contains("\"success\": false", json);
        Assert.Contains("missing.sql", json);
    }

    [Fact]
    public async Task A_Destructive_Writer_Hook_Is_Refused()
    {
        var yaml = "main:\n  input: \"generate:5\"\n  output: \"duck:t.duckdb\"\n  provider-options:\n    duck-writer:\n      pre-exec: \"DROP TABLE sales\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.Contains("\"stage\": \"safety\"", json);
    }

    [Fact]
    public async Task A_Destructive_Hook_File_Is_Refused()
    {
        var file = Path.Combine(_dir, "hook.sql");
        await File.WriteAllTextAsync(file, "TRUNCATE sales", Ct);
        var yaml = $"main:\n  input: \"generate:5\"\n  output: \"duck:t.duckdb\"\n  provider-options:\n    duck-writer:\n      pre-exec: \"@{file}\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.Contains("\"stage\": \"safety\"", json);
    }

    [Fact]
    public async Task A_Destructive_Verb_In_A_Sql_Processor_Is_Refused()
    {
        var yaml = "a:\n  input: \"generate:5\"\nb:\n  from: a\n  output: \"null:\"\n  provider-options:\n    sql:\n      query: \"DELETE FROM a\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.Contains("\"stage\": \"safety\"", json);
    }

    [Fact]
    public async Task The_Studio_Refusal_Does_Not_Send_The_Person_To_A_Flag_The_Command_Line_Lacks()
    {
        var yaml = "main:\n  input: \"generate:5\"\n  output: \"duck:t.duckdb\"\n  provider-options:\n    duck-writer:\n      pre-exec: \"DROP TABLE sales\"\n";

        var refusal = await Assert.ThrowsAsync<InvalidOperationException>(() => Engine().RunAsync(yaml, StudioBindings.None, Ct));

        Assert.Contains("DROP", refusal.Message);
        Assert.DoesNotContain("--allow", refusal.Message);
        Assert.Contains("The studio cannot permit what the policy refuses", refusal.Message);
        Assert.Contains("run the saved job from a terminal with dtpipe --job", refusal.Message);
    }

    [Fact]
    public async Task A_Verb_Spelled_In_A_Connection_String_Is_Not_Destructive_SQL()
    {
        var yaml = "main:\n  input: \"generate:5\"\n  output: \"duck:update.duckdb\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.DoesNotContain("\"stage\": \"safety\"", json);
    }

    [Fact]
    public async Task An_Object_Storage_Connection_String_Is_Refused_Without_AllowNetwork()
    {
        var yaml = "main:\n  input: \"s3://bucket/events.parquet\"\n  output: \"null:\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.Contains("\"stage\": \"safety\"", json);
        AssertTheToolCarriesTheAdvice(json, "allowNetwork");
    }

    [Fact]
    public async Task A_Destructive_Verb_Is_Refused_With_The_Tool_Argument_That_Permits_It()
    {
        var yaml = "main:\n  input: \"generate:5\"\n  output: \"duck:t.duckdb\"\n  provider-options:\n    duck-writer:\n      pre-exec: \"DROP TABLE sales\"\n";

        var json = await Mcp().ExecuteYamlJob(yaml, ct: Ct);

        Assert.Contains("\"stage\": \"safety\"", json);
        AssertTheToolCarriesTheAdvice(json, "allowDestructive");
    }

    /// <summary>
    /// Shape of the refusal: the policy says what it refuses, and the tool, which is the host that
    /// can lift it, says how. The violation names no flag and no argument; the message names the
    /// arguments the tool takes, and not a command-line flag the host lacks.
    /// </summary>
    private static void AssertTheToolCarriesTheAdvice(string json, string argument)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var root = doc.RootElement;

        foreach (var violation in root.GetProperty("violation").EnumerateArray())
        {
            Assert.DoesNotContain("--allow", violation.GetString());
            Assert.DoesNotContain("allowDestructive", violation.GetString());
            Assert.DoesNotContain("allowNetwork", violation.GetString());
        }

        var message = root.GetProperty("message").GetString();
        Assert.Contains(argument, message);
        Assert.DoesNotContain("--allow", message);
    }
}
