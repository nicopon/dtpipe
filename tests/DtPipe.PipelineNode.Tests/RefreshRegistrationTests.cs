using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.FlowControl;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A replica takes the groups its identity holds now with the client's own call, whatever it is doing: the client keeps its
/// connection and the transfers on it, the hub judges them again against the new groups, and an identity that holds no
/// group is refused without the hub forgetting the groups it had.
/// </summary>
public class RefreshRegistrationTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(15);

    private static async Task<(CoordinatorTestHost Host, MutableGroups Rights, TestJwt Jwt)> StartAsync(Action<FlowControlOptions>? flow = null)
    {
        var jwt = new TestJwt();
        var rights = new MutableGroups();
        var host = await CoordinatorTestHost.StartAsync(
            flow ?? (_ => { }),
            services => services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider>(rights)),
            jwt);
        return (host, rights, jwt);
    }

    private static Task<PipelineNode> ConnectAsync(CoordinatorTestHost host, TestJwt jwt, string subject) =>
        PipelineNode.ConnectAsync(new NodeClientOptions
        {
            HubUrl = host.Url,
            AccessTokenProvider = () => Task.FromResult(jwt.Mint(subject, "unused", TimeSpan.FromHours(1))),
        });

    private static async Task<ClientRegistration> RegistrationOfAsync(CoordinatorTestHost host, PipelineNode node) =>
        (await host.Host.Services.GetRequiredService<IStateStore>().GetClientAsync(node.ClientId))!;

    private static string GroupsOf(ClientRegistration registration) => string.Join(",", registration.Groups.Order());

    private static async Task<bool> EventuallyAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline) return false;
            await Task.Delay(50);
        }
        return true;
    }

    /// <summary>
    /// The call belongs to a client that is connected: one the hub cannot be reached from says so at once, and the caller
    /// asks again once the connection is back. It does not wait for a connection to come.
    /// </summary>
    [Fact]
    public async Task WithTheConnectionDown_TheCallIsRefusedAtOnce()
    {
        var (host, rights, jwt) = await StartAsync();
        await using var _host = host;
        rights.BySubject["node-x"] = ["before"];
        await using var node = await ConnectAsync(host, jwt, "node-x");
        await host.Host.StopAsync();
        Assert.True(await EventuallyAsync(async () => await node.IsRegisteredAsync() is null), "the connection never went down");

        await Assert.ThrowsAsync<InvalidOperationException>(() => node.RefreshRegistrationAsync().WaitAsync(Wait));
    }

    /// <summary>
    /// An identity that holds no group cannot register, and a refused call writes nothing: the hub keeps the groups it
    /// had, on the connection it had, until the client's next token renewal withdraws them.
    /// </summary>
    [Fact]
    public async Task AnIdentityWithNoGroup_IsRefused_AndTheHubKeepsWhatItHeld()
    {
        var (host, rights, jwt) = await StartAsync();
        await using var _host = host;
        rights.BySubject["node-x"] = ["before"];
        await using var node = await ConnectAsync(host, jwt, "node-x");
        var held = await RegistrationOfAsync(host, node);

        rights.BySubject["node-x"] = [];
        await Assert.ThrowsAsync<HubException>(() => node.RefreshRegistrationAsync());

        var after = await RegistrationOfAsync(host, node);
        Assert.Equal("before", GroupsOf(after));
        Assert.Equal(held.ConnectionId, after.ConnectionId);
        Assert.Null(after.DisconnectedAtUtc);
    }

    /// <summary>A client the hub has forgotten (a coordinator that lost its store) is registered again by the same call, with the groups of the moment.</summary>
    [Fact]
    public async Task AClientTheHubForgot_IsRegisteredAgainByTheCall()
    {
        var (host, rights, jwt) = await StartAsync();
        await using var _host = host;
        rights.BySubject["node-x"] = ["before"];
        await using var node = await ConnectAsync(host, jwt, "node-x");
        await host.Host.Services.GetRequiredService<IStateStore>().DeleteClientAsync(node.ClientId);
        Assert.False(await node.IsRegisteredAsync());

        rights.BySubject["node-x"] = ["after"];
        await node.RefreshRegistrationAsync();

        Assert.True(await node.IsRegisteredAsync());
        Assert.Equal("after", GroupsOf(await RegistrationOfAsync(host, node)));
    }

    /// <summary>
    /// A refresh with transfers open does not restart the client's connection: when the call returns, the hub has judged
    /// the transfers again, kept the one the new groups still allow and ended the one they do not.
    /// </summary>
    [Fact]
    public async Task ARefreshWithTransfersOpen_KeepsTheConnection_AndEndsTheTransfersTheNewGroupsRefuse()
    {
        var (host, rights, jwt) = await StartAsync(o =>
        {
            o.Groups["old"] = new GroupAccess { CanSendTo = ["keep", "drop"] };
            o.Groups["new"] = new GroupAccess { CanSendTo = ["keep"] };
            o.Groups["keep"] = new GroupAccess { CanSendTo = [] };
            o.Groups["drop"] = new GroupAccess { CanSendTo = [] };
        });
        await using var _host = host;
        rights.BySubject["sender"] = ["old"];
        rights.BySubject["kept"] = ["keep"];
        rights.BySubject["dropped"] = ["drop"];
        await using var sender = await ConnectAsync(host, jwt, "sender");
        await using var kept = await ConnectAsync(host, jwt, "kept");
        await using var dropped = await ConnectAsync(host, jwt, "dropped");
        var initiator = host.Host.Services.GetRequiredService<ITransferInitiator>();
        var transfers = host.Host.Services.GetRequiredService<ITransferManager>();
        var allowedTransfer = await initiator.InitTransferAsync(sender.ClientId, kept.ClientId, batchSize: 8, timeoutMs: 60_000);
        var refusedTransfer = await initiator.InitTransferAsync(sender.ClientId, dropped.ClientId, batchSize: 8, timeoutMs: 60_000);
        var held = await RegistrationOfAsync(host, sender);

        rights.BySubject["sender"] = ["new"];
        await sender.RefreshRegistrationAsync();

        var after = await RegistrationOfAsync(host, sender);
        Assert.Equal("new", GroupsOf(after));
        Assert.Equal(held.ConnectionId, after.ConnectionId);
        var open = transfers.GetAllParticipants();
        Assert.DoesNotContain(refusedTransfer, open.Keys);
        Assert.Contains(allowedTransfer, open.Keys);
    }
}
