using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using DtPipe.Coordinator.Tests.Infrastructure;
using Xunit;

namespace DtPipe.Coordinator.Tests;

public class NodeRegistryTests
{
    [Fact]
    public void Register_SameNameFromADifferentConnection_Throws()
    {
        var registry = new NodeRegistry();
        registry.Register(Guid.NewGuid(), "conn1", "B");

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Register(Guid.NewGuid(), "conn2", "B"));
        Assert.Contains("B", ex.Message);
    }

    [Fact]
    public void Register_SameNameFromTheSameConnection_IsIdempotent()
    {
        var registry = new NodeRegistry();
        var clientId = Guid.NewGuid();
        registry.Register(clientId, "conn1", "B");
        registry.Register(clientId, "conn1", "B");

        Assert.Equal("conn1", registry.TryGetByFragment("B")!.ConnectionId);
    }

    /// <summary>
    /// A disconnect and the re-registration that replaces it race by nature. Once a new connection
    /// has reclaimed a fragment name, an <c>Unregister</c> for the old, now-unrelated connection must
    /// not drop the new entry - the deterrent case: a naive <c>Unregister</c> that removes by
    /// fragment name alone orphans the new connection, and its next lookup throws
    /// <see cref="KeyNotFoundException"/> instead of finding it.
    /// </summary>
    [Fact]
    public void Unregister_ForAConnectionNoLongerOwningTheName_DoesNotOrphanTheNewOwner()
    {
        var registry = new NodeRegistry();
        registry.Register(Guid.NewGuid(), "conn1", "B");
        registry.Unregister("conn1");

        var clientId2 = Guid.NewGuid();
        registry.Register(clientId2, "conn2", "B");
        registry.Unregister("conn1"); // late or duplicate

        var node = registry.TryGetByFragment("B");
        Assert.NotNull(node);
        Assert.Equal("conn2", node!.ConnectionId);
        Assert.Equal(clientId2, registry.TryGetByConnection("conn2")!.ClientId);
    }

    [Fact]
    public async Task Register_SameFragmentFromTwoLiveConnections_RefusesTheSecond()
    {
        await using var host = await CoordinatorTestHost.StartAsync(_ => { });
        await using var a = host.CreateClient();
        await using var b = host.CreateClient();
        await a.ConnectAsync();
        await b.ConnectAsync();

        await a.ControlConnection.InvokeAsync("Register", "B");

        var ex = await Assert.ThrowsAsync<HubException>(
            () => b.ControlConnection.InvokeAsync("Register", "B"));
        Assert.Contains("B", ex.Message);
    }
}
