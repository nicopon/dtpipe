using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Client.SignalR;
using TransportR.Client.SignalR.Services;
using TransportR.FlowControl;
using TransportR.Hub.SignalR;
using TransportR.Serialization.MessagePack;

namespace DtPipe.Coordinator.Tests.Infrastructure;

/// <summary>
/// A coordinator hosted in-process on Kestrel bound to a random port, sharing
/// <c>AddCoordinatorHub</c> with <c>DtPipe.Coordinator.Program</c> but not its dev mode defaults or
/// its hosting model: dev mode here carries a default group so a client that supplies none can
/// still connect, fragments bound to no identity are admitted, and the flow control matrix is
/// caller-supplied instead of empty. A real port,
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
                        // After AddCoordinatorHub: lets a test override NodeRegistryOptions or
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
                        app.UseEndpoints(endpoints => endpoints.MapDataHub(requireAuth: jwt is not null));
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

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
    }
}
