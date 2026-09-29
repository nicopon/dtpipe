using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TransportR.Abstractions;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

namespace DtPipe.PipelineNode.Tests.Infrastructure;

/// <summary>
/// A bare TransportR hub on a random Kestrel port, with no coordinator: transfers are opened and
/// terminated by resolving <see cref="ITransferInitiator"/>/<see cref="ITransferTerminator"/>
/// straight off <see cref="Host"/>, the way a coordinator would from inside its own process. Every
/// client, including a real <see cref="DtPipe.PipelineNode.PipelineNode"/>, connects the same way
/// it would against a real coordinator: through <c>TransportRClientBuilder&lt;T&gt;</c> over HTTP.
/// Started with a <see cref="TestJwt"/> the hub runs in production mode instead: every client is
/// identified by a bearer token signed with that key, and its group is read from the token.
/// </summary>
public sealed class NodeTestHost : IAsyncDisposable
{
    public IHost Host { get; }
    public string Url { get; }

    private NodeTestHost(IHost host, string url)
    {
        Host = host;
        Url = url;
    }

    public static async Task<NodeTestHost> StartAsync(TestJwt? jwt = null)
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
                        // AddDataHub() alone registers no IHubProgressMonitor, and SignalRDataServer
                        // requires one: without it the hub accepts a transfer and then returns 500 on
                        // every stream request instead of forwarding bytes.
                        services.AddSingleton<IHubProgressMonitor, SilentHubProgressMonitor>();
                        var hub = services.AddDataHub().UseHub<TestHub>();
                        if (jwt is null)
                        {
                            hub.UseDevMode(options =>
                            {
                                options.AllowAnonymous = true;
                                options.DefaultGroups = ["test"];
                            });
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

        return new NodeTestHost(host, url);
    }

    public ITransferInitiator TransferInitiator => Host.Services.GetRequiredService<ITransferInitiator>();
    public ITransferTerminator TransferTerminator => Host.Services.GetRequiredService<ITransferTerminator>();

    public async ValueTask DisposeAsync()
    {
        await Host.StopAsync();
        Host.Dispose();
    }
}

file sealed class SilentHubProgressMonitor : IHubProgressMonitor
{
    public void InitializeTransfer(string transferId) { }
    public void OnBytesTransferred(long bytes) { }
    public void CompleteTransfer(string transferId) { }
}
