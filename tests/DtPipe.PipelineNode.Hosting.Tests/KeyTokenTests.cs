using System.Net;
using System.Net.Sockets;
using DtPipe.Coordinator.Server;
using DtPipe.Coordinator.Server.Access;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>
/// A node gets its token with a key from the coordinator's own issuer through the client it uses for any provider, unchanged:
/// <c>Oidc:Authority</c> names the coordinator, or, left out, the coordinator's metadata names its issuer first when it trusts no
/// provider; the key is the client secret, or <c>@path</c> to the file that holds it.
/// </summary>
public sealed class KeyTokenTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// A coordinator listening on <paramref name="url"/>, with its state in SQLite under a fresh directory. With an external provider no
    /// request here reaches; without one, the deployment gives it its administrator and <paramref name="bootstrap"/>.
    /// </summary>
    private sealed class Coordinator(string url, string state, bool provider = true, IReadOnlyDictionary<string, string>? bootstrap = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            if (provider)
            {
                builder.UseSetting("Coordinator:Authority", "http://idp.invalid");
                builder.UseSetting("Coordinator:Ui:ClientSecret", "ui-secret");
            }
            else
            {
                // The test host hands its settings over as command-line arguments, where a key itself is refused: a file's path is not one.
                var admin = Path.Combine(state, "admin.key");
                File.WriteAllText(admin, ClearKey.New("boss").Reveal());
                builder.UseSetting("Coordinator:Bootstrap:AdminKey", "@" + admin);
            }
            foreach (var (key, value) in bootstrap ?? new Dictionary<string, string>()) builder.UseSetting("Coordinator:Bootstrap:" + key, value);
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

    [Fact]
    public async Task A_Node_With_No_Authority_Finds_The_Issuer_Of_A_Coordinator_That_Trusts_No_Provider_And_Reads_Its_Key_From_A_File()
    {
        var port = FreePort();
        var url = $"http://127.0.0.1:{port}";
        var state = Path.Combine(Path.GetTempPath(), "key-token-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(state);
        var keyFile = Path.Combine(state, "runner.key");
        await File.WriteAllTextAsync(keyFile, ClearKey.New("runner").Reveal() + "\n", Ct);
        try
        {
            // The coordinator and the node read the same file, as a deployment's bootstrap service would give it to both.
            using var coordinator = new Coordinator(url, state, provider: false, new Dictionary<string, string>
            {
                ["Identities:0:Id"] = "runner",
                ["Identities:0:Groups:0"] = "runners",
                ["Identities:0:Key"] = "@" + keyFile,
            });
            coordinator.UseKestrel(port);
            coordinator.StartServer();

            using var tokens = new OidcTokenClient(new NodeOidcOptions { ClientSecret = "@" + keyFile }, "runner", url);
            var token = await tokens.GetAsync();

            Assert.Equal(url, new Microsoft.IdentityModel.JsonWebTokens.JsonWebToken(token).Issuer);
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

    [Fact]
    public void A_Secret_Read_From_A_File_Names_Neither_The_File_Nor_What_It_Holds()
    {
        var dir = Path.Combine(Path.GetTempPath(), "node-secret-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var file = Path.Combine(dir, "secret");
            File.WriteAllText(file, "  s3cret\n");
            Assert.Equal("s3cret", SecretReference.Read("@" + file, "DtPipeNode:Oidc:ClientSecret"));
            Assert.Equal("plain", SecretReference.Read("plain", "DtPipeNode:Oidc:ClientSecret"));

            var pasted = ClearKey.New("runner").Reveal();
            var missing = Assert.Throws<InvalidOperationException>(() => SecretReference.Read("@" + pasted, "DtPipeNode:Oidc:ClientSecret"));
            Assert.Contains("DtPipeNode:Oidc:ClientSecret names a file (@…) that cannot be read", missing.Message);
            Assert.DoesNotContain(pasted, missing.Message);
            File.WriteAllText(file, " \n");
            Assert.Contains("is empty", Assert.Throws<InvalidOperationException>(() => SecretReference.Read("@" + file, "DtPipeNode:Oidc:ClientSecret")).Message);
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
