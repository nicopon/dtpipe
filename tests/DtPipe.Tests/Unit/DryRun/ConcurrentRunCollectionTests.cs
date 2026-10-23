using DtPipe.Cli;
using DtPipe.Cli.Studio;
using DtPipe.Tests.Unit.Cli;
using AwesomeAssertions;
using DtPipe.Cli.Mcp;
using DtPipe.Sessions;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.DryRun;

/// <summary>
/// A sample collects what its own run reports and nothing a real run beside it publishes: the agent
/// runs the tools of one turn in parallel, so an <c>execute-yaml-job</c> and a <c>dry-run</c> overlap.
/// </summary>
[Collection(SessionStateCollection.Name)]
public sealed class ConcurrentRunCollectionTests : IDisposable
{
    private readonly string _tmp;
    private readonly string _cwd;
    private readonly string? _savedState;

    public ConcurrentRunCollectionTests()
    {
        _cwd = Directory.GetCurrentDirectory();
        _tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_concrun_{Guid.NewGuid():N}");
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

    private static IServiceProvider Provider()
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        services.AddSingleton<IAnsiConsole>(AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new StringWriter()), Ansi = AnsiSupport.No, Interactive = InteractionSupport.No,
        }));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task A_Real_Linear_Run_Beside_A_Sample_Leaves_The_Sample_Its_Own_Branch_And_Keys()
    {
        var sp = Provider();
        var tools = sp.GetRequiredService<JobAnalysis>();
        var ct = TestContext.Current.CancellationToken;

        // A real run that commits a checkpoint, and a sample of another linear job, started together.
        var real = StudioEngines.Real(sp).RunAsync(
            "real:\n  input: generate:200000\n  checkpoint: held\n  output: \"null:\"\n", StudioBindings.None, ct);
        var sample = await tools.SampleAsync("sample:\n  input: generate:50\n  output: \"null:\"\n", rows: 5, shownRows: 5, ct);
        var outcome = await real;
        outcome.ExitCode.Should().Be(0);

        var branches = System.Text.Json.JsonDocument.Parse(sample).RootElement.GetProperty("branches");
        branches.EnumerateArray().Select(b => b.GetProperty("branch").GetString())
            .Should().Equal(["sample"], "the sample reports its own branch, not the real run's");
        branches[0].GetProperty("checkpoint").ValueKind.Should().Be(System.Text.Json.JsonValueKind.Null,
            "the sample took no checkpoint, and the real run's key is not its own");
        outcome.Checkpoints.Keys.Should().ContainSingle().Which.Should().Be("real", "the real run reads the key of its own checkpoint");
    }
}
