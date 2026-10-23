using System.Net;
using System.Net.Sockets;
using DtPipe.Coordinator.Server;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>
/// A node gets its token with a key from the coordinator's own issuer through the client it uses for any provider, unchanged:
/// <c>Oidc:Authority</c> names the coordinator, and the key is the client secret.
/// </summary>
public sealed class KeyTokenTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A coordinator listening on <paramref name="url"/>, with its state in SQLite under a fresh directory.</summary>
    private sealed class Coordinator(string url, string state) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            // An external provider no request here reaches: the node's identity is verified by a key.
            builder.UseSetting("Coordinator:Authority", "http://idp.invalid");
            builder.UseSetting("Coordinator:Ui:ClientSecret", "ui-secret");
            builder.UseSetting("Coordinator:StateDirectory", state);
            builder.UseSetting("Coordinator:Initialize", "true");
            builder.UseSetting("Coordinator:PublicUrl", url);
            builder.UseSetting("Coordinator:Auth:AllowHttpKeys", "true");
        }
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    [Fact]
    public async Task A_Node_Exchanges_Its_Key_At_The_Coordinator_With_Its_Ordinary_Token_Client()
    {
        var port = FreePort();
        var url = $"http://127.0.0.1:{port}";
        var state = Path.Combine(Path.GetTempPath(), "key-token-" + Guid.NewGuid().ToString("N"));
        try
        {
            using var coordinator = new Coordinator(url, state);
            coordinator.UseKestrel(port);
            coordinator.StartServer();
            var rights = coordinator.Services.GetRequiredService<RightsStore>();
            rights.AddIdentity(new Identity("edge", "edge", "", ["crm"]) { Auth = new IdentityAuth(AuthModes.Key, []) });
            var (key, _) = rights.AddKey("edge", null, null);

            using var tokens = new OidcTokenClient(new NodeOidcOptions { Authority = url, ClientSecret = key.Reveal() }, "edge", url);
            var token = await tokens.GetAsync();

            using var http = new HttpClient();
            http.DefaultRequestHeaders.Authorization = new("Bearer", token);
            using var negotiated = await http.PostAsync(url + "/node-channel/negotiate?negotiateVersion=1", null, Ct);
            Assert.Equal(HttpStatusCode.OK, negotiated.StatusCode);
        }
        finally
        {
            if (Directory.Exists(state)) Directory.Delete(state, recursive: true);
        }
    }
}
