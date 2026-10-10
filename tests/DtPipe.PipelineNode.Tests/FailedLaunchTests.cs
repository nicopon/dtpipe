using DtPipe.Coordinator.Tests.Infrastructure;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A launch that cannot start its child (the binary is gone, the process will not spawn) leaves a node
/// with nothing to tear down: disposing it still succeeds, so whoever holds it can finish unregistering.
/// </summary>
public class FailedLaunchTests : IDisposable
{
    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("dtpipe-failed-launch-");

    public void Dispose() => _dir.Delete(recursive: true);

    [Fact]
    public async Task ANodeWhoseChildCouldNotBeStarted_IsDisposedWithoutError()
    {
        await using var host = await CoordinatorTestHost.StartAsync(
            o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        var job = Path.Combine(_dir.FullName, "job.yaml");
        await File.WriteAllTextAsync(job, "a:\n  input: generate:1\n  output: arrow:-\n", TestContext.Current.CancellationToken);
        var node = await PipelineNode.ConnectAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            DtPipeExecutable = Path.Combine(_dir.FullName, "no-such-dtpipe"),
            FragmentJobPath = job,
            Edges = [],
        }, "A", ct: TestContext.Current.CancellationToken);

        await Assert.ThrowsAnyAsync<Exception>(() => node.LaunchAsync("r1"));
        Assert.False(node.IsLaunched);

        await node.DisposeAsync();
    }
}
