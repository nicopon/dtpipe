using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.Client.Services;
using TransportR.Interfaces;
using TransportR.Models;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// How a node's TransportR client takes the groups its identity holds now. The hub writes a client's
/// groups when the client registers; a coordinator reads them from its rights at that moment, as
/// <c>RightsIdentityProvider</c> does, so a client that never registers again keeps the groups it had. The client's own
/// call has the hub read them again.
/// </summary>
public class GroupRefreshTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(15);

    private static async Task<(CoordinatorTestHost Host, MutableGroups Rights, string Token)> StartAsync()
    {
        var jwt = new TestJwt();
        var rights = new MutableGroups();
        rights.BySubject["node-x"] = ["before"];
        var host = await CoordinatorTestHost.StartAsync(_ => { }, services => services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider>(rights)), jwt);
        return (host, rights, jwt.Mint("node-x", "unused", TimeSpan.FromHours(1)));
    }

    /// <summary>The client pushes the token it holds as soon as it connects, and the hub reads the groups again for it: let that settle.</summary>
    private static Task SettleAsync() => Task.Delay(TimeSpan.FromSeconds(2));

    /// <summary>
    /// Refreshing a connected client has the hub take the groups its identity holds then, under the same client id: the
    /// call returns once the hub has written them.
    /// </summary>
    [Fact]
    public async Task RefreshingTheRegistration_TakesTheGroupsOfTheMoment_UnderTheSameClientId()
    {
        var (host, rights, token) = await StartAsync();
        await using var _host = host;
        await using var client = host.CreateClient(token);
        await client.ConnectAsync();
        await SettleAsync();
        var store = host.Host.Services.GetRequiredService<IStateStore>();
        var before = await store.GetClientAsync(client.ClientId);

        rights.BySubject["node-x"] = ["after"];
        await client.RefreshRegistrationAsync();

        var after = await store.GetClientAsync(client.ClientId);
        Assert.Equal("after", string.Join(",", after!.Groups.Order()));
        Assert.Equal(before!.Subject, after.Subject);
        Assert.Null(after.DisconnectedAtUtc);
    }
}
