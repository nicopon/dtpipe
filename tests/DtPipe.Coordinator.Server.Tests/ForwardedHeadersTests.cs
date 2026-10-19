using System.Net;
using System.Text.Json;
using DtPipe.Jobs.Coordinator;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// Behind a proxy that terminates TLS the server sees plain HTTP: what a person's sign-in and the studio's discovery
/// are told about the scheme and the host must still be the proxy's, and only a trusted proxy is believed.
/// </summary>
public sealed class ForwardedHeadersTests
{
    private const string Proxy = "10.0.0.9";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static HttpClient Behind(CoordinatorHost host)
    {
        var client = host.Anonymous();
        client.DefaultRequestHeaders.Add("X-Forwarded-Proto", "https");
        client.DefaultRequestHeaders.Add("X-Forwarded-Host", "coordinator.example:8443");
        return client;
    }

    [Fact]
    public async Task Behind_A_Trusted_Https_Proxy_No_Http_Scheme_Leaves_The_Server()
    {
        using var host = new CoordinatorHost(trustedProxies: ["10.0.0.0/24"], peer: Proxy);
        var client = Behind(host);

        var metadata = JsonDocument.Parse(await client.GetStringAsync(CoordinatorApi.Metadata, Ct)).RootElement;
        Assert.Equal("https://coordinator.example:8443", metadata.GetProperty("resource").GetString());

        using var challenged = await client.GetAsync("/api/me", Ct);
        Assert.Contains("resource_metadata=\"https://coordinator.example:8443" + CoordinatorApi.Metadata + "\"", challenged.Headers.WwwAuthenticate.ToString());

        using var signIn = await client.GetAsync("/", Ct);
        Assert.Equal(HttpStatusCode.Redirect, signIn.StatusCode);
        var query = System.Web.HttpUtility.ParseQueryString(signIn.Headers.Location!.Query);
        Assert.Equal("https://coordinator.example:8443/signin-oidc", query["redirect_uri"]);

        // The correlation and nonce cookies ride the redirect back from the provider: Secure, or a browser on https would drop them.
        var cookies = signIn.Headers.GetValues("Set-Cookie").ToList();
        Assert.NotEmpty(cookies);
        Assert.All(cookies, cookie => Assert.Contains("secure", cookie, StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("10.0.1.9")]  // a peer outside the trusted network
    [InlineData("127.0.0.1")] // the loopback is trusted only when it is listed
    public async Task A_Peer_That_Is_Not_Trusted_Changes_Nothing(string peer)
    {
        using var host = new CoordinatorHost(trustedProxies: ["10.0.0.0/24"], peer: peer);
        var client = Behind(host);

        var metadata = JsonDocument.Parse(await client.GetStringAsync(CoordinatorApi.Metadata, Ct)).RootElement;
        Assert.Equal("http://localhost", metadata.GetProperty("resource").GetString());

        using var signIn = await client.GetAsync("/", Ct);
        Assert.Equal("http://localhost/signin-oidc", System.Web.HttpUtility.ParseQueryString(signIn.Headers.Location!.Query)["redirect_uri"]);
        Assert.All(signIn.Headers.GetValues("Set-Cookie"), cookie => Assert.DoesNotContain("secure", cookie, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task With_No_Trusted_Proxy_No_Forwarded_Header_Is_Read()
    {
        using var host = new CoordinatorHost(peer: "127.0.0.1");
        var metadata = JsonDocument.Parse(await Behind(host).GetStringAsync(CoordinatorApi.Metadata, Ct)).RootElement;
        Assert.Equal("http://localhost", metadata.GetProperty("resource").GetString());
    }

    [Theory]
    [InlineData("10.0.0.0/33")]
    [InlineData("proxy.internal")]
    [InlineData("10.0.0.1/8x")]
    public void A_Malformed_Entry_Stops_The_Start_And_Is_Named(string entry)
    {
        var failure = Assert.ThrowsAny<Exception>(() => TrustedProxies.OptionsFor([entry]));
        Assert.Contains(entry, failure.Message);
    }

    [Fact]
    public void An_Address_And_A_Network_Are_Both_Read()
    {
        var options = TrustedProxies.OptionsFor(["192.168.0.0/16", "::1", "10.1.2.3"])!;
        Assert.Single(options.KnownIPNetworks);
        Assert.Equal(2, options.KnownProxies.Count);
    }
}
