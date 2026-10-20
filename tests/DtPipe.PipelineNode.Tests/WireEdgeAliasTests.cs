using DtPipe.PipelineNode.Tests.Infrastructure;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// The edges a run declares are checked where they are named: a launch with one alias twice is refused, and a wire for an alias the
/// run does not declare is refused before it claims the transfer's handle, so the handle stays for the edge that does exist.
/// </summary>
public class WireEdgeAliasTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task AWireForAnAliasTheRunDoesNotDeclare_IsRefusedBeforeItClaimsTheHandle()
    {
        using var fixture = SplitFixture.Create(rowCount: 100);
        await using var host = await NodeTestHost.StartAsync();

        await using var producer = await HandWiredNode.StartAsync(
            new NodeClientOptions { HubUrl = host.Url },
            new FragmentLaunch
            {
                DtPipeExecutable = DtPipeExecutableLocator.Path,
                FragmentJobPath = fixture.ProducerJobPath,
                Edges = [new EdgeBinding("main", EdgeDirection.Outbound)],
            });
        await using var consumer = await HandWiredNode.StartAsync(
            new NodeClientOptions { HubUrl = host.Url },
            new FragmentLaunch
            {
                DtPipeExecutable = DtPipeExecutableLocator.Path,
                FragmentJobPath = fixture.ConsumerJobPath,
                Edges = [new EdgeBinding("main", EdgeDirection.Inbound)],
            });
        var transferId = await host.TransferInitiator.InitTransferAsync(
            producer.ClientId, consumer.ClientId, batchSize: 8, timeoutMs: 20_000);

        await Assert.ThrowsAsync<ArgumentException>(() => consumer.WireAsync("not-declared", transferId).WaitAsync(Bound));

        await Task.WhenAll(
            producer.WireAsync("main", transferId).WaitAsync(Bound),
            consumer.WireAsync("main", transferId).WaitAsync(Bound));
        var exitCodes = await Task.WhenAll(
            producer.RunToCompletionAsync().WaitAsync(Bound),
            consumer.RunToCompletionAsync().WaitAsync(Bound));
        Assert.Equal([0, 0], exitCodes);
    }

    [Fact]
    public async Task ALaunchWithTwoEdgesOfOneAlias_IsRefusedNamingTheAlias()
    {
        using var fixture = SplitFixture.Create(rowCount: 10);
        await using var host = await NodeTestHost.StartAsync();
        await using var node = await PipelineNode.ConnectAsync(new NodeClientOptions { HubUrl = host.Url }, ct: TestContext.Current.CancellationToken);

        var ex = Assert.Throws<ArgumentException>(() => node.Launch(new FragmentLaunch
        {
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = fixture.ProducerJobPath,
            Edges = [new EdgeBinding("main", EdgeDirection.Inbound), new EdgeBinding("main", EdgeDirection.Outbound)],
        }));

        Assert.Contains("'main' is declared more than once", ex.Message);
    }
}
