using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Interfaces;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>Whether the hub knows a replica's TransportR client, which a replica asks before it trusts the data plane to carry a run.</summary>
public class RegistrationTests
{
    [Fact]
    public async Task AClientTheHubKnows_IsRegistered_AndOneItHasForgotten_IsNot()
    {
        await using var host = await CoordinatorTestHost.StartAsync(o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        await using var node = await PipelineNode.ConnectAsync(new NodeClientOptions { HubUrl = host.Url }, ct: TestContext.Current.CancellationToken);

        Assert.True(await node.IsRegisteredAsync(TestContext.Current.CancellationToken));

        await host.Host.Services.GetRequiredService<IStateStore>().DeleteClientAsync(node.ClientId);

        Assert.False(await node.IsRegisteredAsync(TestContext.Current.CancellationToken));
    }
}
