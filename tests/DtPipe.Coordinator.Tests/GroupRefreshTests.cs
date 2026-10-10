using System.Collections.Concurrent;
using System.Security.Claims;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.Client.SignalR.Services;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// How a node's TransportR client takes the groups its identity holds now. The hub writes a client's
/// groups when the client registers; a coordinator reads them from its rights at that moment, as
/// <c>RightsIdentityProvider</c> does, so a client that never registers again keeps the groups it had.
/// </summary>
public class GroupRefreshTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(15);

    /// <summary>Groups per subject, read at every registration: the rights document of the real coordinator.</summary>
    private sealed class MutableGroups : IClientIdentityProvider
    {
        public readonly ConcurrentDictionary<string, string[]> BySubject = new();
        private readonly JwtIdentityProvider _token = new();

        public ClientIdentity? ResolveIdentity(ClaimsPrincipal? user, Guid? providedClientId, IEnumerable<string>? providedGroups)
        {
            var identity = _token.ResolveIdentity(user, providedClientId, providedGroups);
            if (identity is null) return null;
            return identity with { Groups = BySubject.TryGetValue(identity.Subject ?? "", out var groups) ? groups.ToHashSet() : [] };
        }
    }

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

    private static async Task<string> RegisteredGroupsAsync(CoordinatorTestHost host, Guid clientId) =>
        string.Join(",", ((await host.Host.Services.GetRequiredService<IStateStore>().GetClientAsync(clientId))?.Groups ?? []).Order());

    /// <summary>
    /// A started client cannot be told to register again: <c>ConnectAsync</c> refuses. This is why a node restarts the client's
    /// connection instead (<see cref="StoppingTheControlConnection_MakesTheClientRegisterAgain"/>); the day this test fails,
    /// <c>PipelineNode.RefreshRegistrationAsync</c> can be a plain call.
    /// </summary>
    [Fact]
    public async Task ConnectingAgain_OnAStartedClient_IsRefused()
    {
        var (host, rights, token) = await StartAsync();
        await using var _host = host;
        await using var client = host.CreateClient(token);
        await client.ConnectAsync();
        await SettleAsync();
        Assert.Equal("before", await RegisteredGroupsAsync(host, client.ClientId));

        rights.BySubject["node-x"] = ["after"];
        var ex = await Record.ExceptionAsync(() => client.ConnectAsync());

        Assert.NotNull(ex);
        Assert.Equal("before", await RegisteredGroupsAsync(host, client.ClientId));
    }

    /// <summary>
    /// Stopping the control connection makes the client start it again and register with the groups its identity holds
    /// then, under the same client id: the registration is on a new connection (not merely renewed by the token push the client
    /// makes as it connects), and it holds the new groups.
    /// </summary>
    [Fact]
    public async Task StoppingTheControlConnection_MakesTheClientRegisterAgain()
    {
        var (host, rights, token) = await StartAsync();
        await using var _host = host;
        await using var client = host.CreateClient(token);
        await client.ConnectAsync();
        await SettleAsync();
        var store = host.Host.Services.GetRequiredService<IStateStore>();
        var before = await store.GetClientAsync(client.ClientId);

        rights.BySubject["node-x"] = ["after"];
        await client.ControlConnection.StopAsync();

        var deadline = DateTime.UtcNow + Wait;
        ClientRegistration? after = null;
        while (DateTime.UtcNow < deadline)
        {
            after = await store.GetClientAsync(client.ClientId);
            if (after is { DisconnectedAtUtc: null } && after.ConnectionId != before!.ConnectionId) break;
            await Task.Delay(100);
        }
        Assert.NotEqual(before!.ConnectionId, after!.ConnectionId);
        Assert.Equal("after", string.Join(",", after.Groups.Order()));
        Assert.Equal(before.Subject, after.Subject);
    }
}
