using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A launch that cannot start its child (the binary is gone, the process will not spawn) leaves a replica with
/// nothing to tear down: the run protocol answers that it did not start, and the replica goes on, with nothing in its table.
/// </summary>
public class FailedLaunchTests : IDisposable
{
    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("dtpipe-failed-launch-");

    public void Dispose() => _dir.Delete(recursive: true);

    [Fact]
    public async Task AReplicaWhoseChildCouldNotBeStarted_AnswersThatItDidNotStart_AndKeepsNoRun()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        await using var node = await PipelineNode.ConnectAsync(new NodeClientOptions { HubUrl = host.Url }, ct: TestContext.Current.CancellationToken);
        await using var runs = new ReplicaRuns(
            host.CreateNodeChannel(), node, "A", Path.Combine(_dir.FullName, "no-such-dtpipe"), new Dictionary<string, string>(), _dir.FullName,
            spec => FragmentIntake.JobOf(spec, FragmentPolicy.AnyFragment, null, (_, _) => null, (_, _) => null, []),
            _ => NullLogger.Instance, NullLogger.Instance, TimeSpan.FromMinutes(5));

        var result = await runs.LaunchAsync(new LaunchRequest("r1", new FragmentSpec("A", "v", [], Yaml: "a:\n  input: generate:1\n  output: arrow:-\n"), "token-1"));

        Assert.False(result.Started);
        Assert.Contains("could not start dtpipe", result.Refusal);
        Assert.Equal(0, runs.Count);
        Assert.Null(runs.Find("A"));
        Assert.Empty(Directory.EnumerateFiles(_dir.FullName, "*.yaml"));
    }

    [Fact]
    public async Task ANodeWhoseChildCouldNotBeStarted_IsDisposedWithoutError()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        var job = Path.Combine(_dir.FullName, "job.yaml");
        await File.WriteAllTextAsync(job, "a:\n  input: generate:1\n  output: arrow:-\n", TestContext.Current.CancellationToken);
        var node = await PipelineNode.ConnectAsync(new NodeClientOptions { HubUrl = host.Url }, ct: TestContext.Current.CancellationToken);

        Assert.ThrowsAny<Exception>(() => node.Launch(new FragmentLaunch
        {
            DtPipeExecutable = Path.Combine(_dir.FullName, "no-such-dtpipe"),
            FragmentJobPath = job,
            Edges = [],
        }));

        await node.DisposeAsync();
    }
}
