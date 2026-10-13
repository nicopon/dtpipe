using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Client.SignalR;
using TransportR.Client.SignalR.Services;
using TransportR.FlowControl;
using TransportR.Hub.SignalR;
using TransportR.Serialization.MessagePack;

namespace DtPipe.Coordinator.Tests.Infrastructure;

/// <summary>
/// The node channel of a test coordinator: a hub whose methods each forward to <see cref="ReplicaProtocol"/>, as the
/// server's does, with the rights and the enrolment left out. The caller is the connection, and its subject is the
/// token's <c>sub</c>, none in dev mode.
/// </summary>
public sealed class TestNodeHub(ReplicaProtocol protocol, AnnouncementGate gate) : Hub
{
    public async Task Announce(NodeAnnouncement announcement)
    {
        gate.Arrived();
        if (gate.Refusing)
        {
            // The connection stays open while the refusal is held, as a coordinator that is slow to refuse leaves it.
            if (gate.RefuseAfter > TimeSpan.Zero) await Task.Delay(gate.RefuseAfter);
            Context.Abort();
            return;
        }
        var subject = Context.User?.FindFirst("sub")?.Value;
        var (replica, _) = await protocol.OnAnnouncedAsync(Context.ConnectionId, subject, announcement, [], Context.Abort);
        if (replica is null) Context.Abort();
    }

    public Task<ExitedAck> Exited(FragmentExit exit) => protocol.OnExitedAsync(Context.ConnectionId, exit);

    public Task Leave()
    {
        protocol.OnLeave(Context.ConnectionId);
        return Task.CompletedTask;
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        protocol.OnDisconnected(Context.ConnectionId);
        return base.OnDisconnectedAsync(exception);
    }
}

/// <summary>
/// What a test coordinator does with an announcement: accepts it, or, once a test sets <see cref="Refusing"/>, closes the
/// connection after <see cref="RefuseAfter"/>, as the real coordinator does with a host whose identity is unknown or disabled.
/// It counts the announcements that reached the node channel.
/// </summary>
public sealed class AnnouncementGate
{
    private int _arrived;
    private volatile bool _refusing;
    private long _refuseAfterTicks;

    public bool Refusing { get => _refusing; set => _refusing = value; }

    public TimeSpan RefuseAfter
    {
        get => TimeSpan.FromTicks(Interlocked.Read(ref _refuseAfterTicks));
        set => Interlocked.Exchange(ref _refuseAfterTicks, value.Ticks);
    }

    public int Announcements => Volatile.Read(ref _arrived);

    internal void Arrived() => Interlocked.Increment(ref _arrived);
}

/// <summary>
/// A coordinator hosted in-process on Kestrel bound to a random port, sharing
/// <c>AddCoordinatorHub</c> with <c>DtPipe.Coordinator.Server</c> but not its dev mode defaults or
/// its hosting model: dev mode here carries a default group so a client that supplies none can
/// still connect, fragments bound to no identity are admitted, and the flow control matrix is
/// caller-supplied instead of empty. Replicas announce themselves on the node channel (<see cref="CreateNodeChannel"/>). A real port,
/// rather than <c>TestServer</c>'s in-memory handler, is what lets a peer connect through
/// <see cref="TransportRClientBuilder{T}"/> - the same public API a real pipeline node uses -
/// instead of the package's internal connection plumbing, which only <c>TransportR.Tests</c> may
/// reference. Started with a <see cref="TestJwt"/> the hub runs in production mode instead: every
/// client is identified by a bearer token signed with that key, attests its subject, and no fragment
/// is admitted without a node identity to bind it to.
/// </summary>
public sealed class CoordinatorTestHost : IAsyncDisposable
{
    public IHost Host { get; }
    public string Url { get; }

    private CoordinatorTestHost(IHost host, string url)
    {
        Host = host;
        Url = url;
    }

    public static async Task<CoordinatorTestHost> StartAsync(
        Action<FlowControlOptions> configureFlowControl, Action<IServiceCollection>? configureServices = null,
        TestJwt? jwt = null)
    {
        var host = new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder
                    .UseKestrel()
                    .UseUrls("http://127.0.0.1:0")
                    .ConfigureServices(services =>
                    {
                        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
                        var hub = services.AddCoordinatorHub(configureFlowControl);
                        if (jwt is null)
                        {
                            hub.UseDevMode(options =>
                            {
                                options.AllowAnonymous = true;
                                options.DefaultGroups = ["test"];
                            });
                            // The hub attests no subject here, so no fragment can be bound to one.
                            services.AddSingleton(new AdmissionOptions { AdmitUnboundFragments = true });
                        }
                        else
                        {
                            services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
                                    .AddJwtBearer(options =>
                                    {
                                        options.MapInboundClaims = false;
                                        options.TokenValidationParameters = jwt.ValidationParameters;
                                    });
                            services.AddAuthorization();
                        }
                        hub.Build();
                        services.AddSingleton<AnnouncementGate>();
                        services.AddNodeLink<TestNodeHub>();
                        // After AddCoordinatorHub: lets a test override ReplicaRegistryOptions or
                        // RunOrchestratorOptions, neither registered by default (both types resolve
                        // their own default when DI has nothing for them).
                        configureServices?.Invoke(services);
                    })
                    .Configure(app =>
                    {
                        app.UseRouting();
                        if (jwt is not null)
                        {
                            app.UseAuthentication();
                            app.UseAuthorization();
                        }
                        app.UseEndpoints(endpoints =>
                        {
                            endpoints.MapDataHub(requireAuth: jwt is not null);
                            var node = endpoints.MapHub<TestNodeHub>(NodeChannel.Path);
                            if (jwt is not null) node.RequireAuthorization();
                        });
                    });
            })
            .Build();

        await host.StartAsync();

        var addresses = host.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!.Addresses;
        var url = addresses.First();

        return new CoordinatorTestHost(host, url);
    }

    /// <summary>
    /// A client standing in for a pipeline node - the only kind of peer a real coordinator ever sees.
    /// <paramref name="token"/> is the bearer token a production-mode host requires.
    /// </summary>
    public SignalRDataClient<string> CreateClient(string? token = null)
    {
        var builder = new TransportRClientBuilder<string>()
            .WithUrl(Url)
            .WithMessagePackSerialization();
        if (token is not null)
            builder.WithTokenProvider(() => Task.FromResult(token));
        return builder.Build();
    }

    /// <summary>A node channel connection, not started: the transport a replica announces itself and reports on.</summary>
    public HubConnection CreateNodeChannel(string? token = null) => new HubConnectionBuilder()
        .WithUrl(Url + NodeChannel.Path, options =>
        {
            if (token is not null) options.AccessTokenProvider = () => Task.FromResult<string?>(token);
        })
        .Build();

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
    }
}
