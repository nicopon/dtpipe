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
/// Runs started together in one process follow each other, each with its own rows and reports: the agent
/// runs the tools of one turn in parallel, so an <c>execute-yaml-job</c> and a <c>dry-run</c> start together.
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
    public async Task Two_Dags_On_One_Alias_Started_Together_Each_Write_Their_Own_Rows()
    {
        var engine = StudioEngines.Real(Provider());
        var ct = TestContext.Current.CancellationToken;
        static string Dag(int rows, string file) => $"a:\n  input: generate:{rows}\nb:\n  from: a\n  output: csv:{file}\n";

        for (var round = 0; round < 5; round++)
        {
            var one = Task.Run(() => engine.RunAsync(Dag(1000, $"one{round}.csv"), StudioBindings.None, ct), ct);
            var two = Task.Run(() => engine.RunAsync(Dag(2000, $"two{round}.csv"), StudioBindings.None, ct), ct);
            var outcomes = await Task.WhenAll(one, two).WaitAsync(TimeSpan.FromSeconds(60), ct);

            outcomes.Select(o => o.ExitCode).Should().Equal([0, 0],
                $"round {round}: {string.Join(" ", outcomes.SelectMany(o => o.Failures.Values))}");
            File.ReadLines($"one{round}.csv").Count().Should().Be(1001, "a header and the first run's rows, none of the other's");
            File.ReadLines($"two{round}.csv").Count().Should().Be(2001);
        }
    }

    [Fact]
    public async Task A_Run_Cancelled_While_Another_Holds_The_Process_Returns_130()
    {
        var engine = StudioEngines.Real(Provider());
        var ct = TestContext.Current.CancellationToken;
        using var stopHeld = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var held = engine.RunAsync("main:\n  input: generate:2000000000\n  output: \"null:\"\n", StudioBindings.None, stopHeld.Token);
        await Task.Delay(500, ct);

        using var stopWaiting = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var waiting = engine.RunAsync("main:\n  input: generate:5\n  output: \"null:\"\n", StudioBindings.None, stopWaiting.Token);
        await Task.Delay(200, ct);
        stopWaiting.Cancel();
        var outcome = await waiting.WaitAsync(TimeSpan.FromSeconds(20), ct);

        held.IsCompleted.Should().BeFalse("the first run still holds the process");
        outcome.ExitCode.Should().Be(130, "a run cancelled before it started is a user cancellation");
        outcome.Fault.Should().BeNull("a linear run cancelled once started throws; this one never started");

        stopHeld.Cancel();
        await held.WaitAsync(TimeSpan.FromSeconds(20), ct);
    }

    [Fact]
    public async Task A_Real_Run_And_A_Sample_Started_Together_Each_Read_Their_Own_Reports()
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
