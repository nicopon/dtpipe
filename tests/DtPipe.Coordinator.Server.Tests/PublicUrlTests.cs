using System.Net;
using System.Text.Json;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// Behind a proxy that terminates TLS the server sees plain HTTP: what a person's sign-in and the studio's discovery are told about
/// the scheme and the host must still be the public address, and no header a caller sends changes it.
/// </summary>
public sealed class PublicUrlTests
{
    private const string Public = "https://coordinator.example:8443";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static HttpClient Forging(CoordinatorHost host)
    {
        var client = host.Anonymous();
        client.DefaultRequestHeaders.Add("X-Forwarded-Proto", "http");
        client.DefaultRequestHeaders.Add("X-Forwarded-Host", "evil.test");
        client.DefaultRequestHeaders.Host = "evil.test";
        return client;
    }

    [Fact]
    public async Task With_A_Public_Url_No_Http_Scheme_And_No_Foreign_Host_Leaves_The_Server()
    {
        using var host = new CoordinatorHost(publicUrl: Public);
        var client = Forging(host);

        var metadata = JsonDocument.Parse(await client.GetStringAsync(CoordinatorApi.Metadata, Ct)).RootElement;
        Assert.Equal(Public, metadata.GetProperty("resource").GetString());

        using var challenged = await client.GetAsync("/api/me", Ct);
        Assert.Contains($"resource_metadata=\"{Public}{CoordinatorApi.Metadata}\"", challenged.Headers.WwwAuthenticate.ToString());

        using var signIn = await client.GetAsync("/", Ct);
        Assert.Equal(HttpStatusCode.Redirect, signIn.StatusCode);
        Assert.Equal(Public + "/signin-oidc", System.Web.HttpUtility.ParseQueryString(signIn.Headers.Location!.Query)["redirect_uri"]);

        // The correlation and nonce cookies ride the redirect back from the provider: Secure, or a browser on https would drop them.
        var cookies = signIn.Headers.GetValues("Set-Cookie").ToList();
        Assert.NotEmpty(cookies);
        Assert.All(cookies, cookie => Assert.Contains("secure", cookie, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task The_Default_Port_Of_The_Scheme_Is_Not_Spelled()
    {
        using var host = new CoordinatorHost(publicUrl: "https://coordinator.example/");
        var metadata = JsonDocument.Parse(await host.Anonymous().GetStringAsync(CoordinatorApi.Metadata, Ct)).RootElement;
        Assert.Equal("https://coordinator.example", metadata.GetProperty("resource").GetString());
    }

    [Fact]
    public async Task Without_A_Public_Url_The_Request_Stands_As_It_Came()
    {
        using var host = new CoordinatorHost();
        var metadata = JsonDocument.Parse(await host.Anonymous().GetStringAsync(CoordinatorApi.Metadata, Ct)).RootElement;
        Assert.Equal("http://localhost", metadata.GetProperty("resource").GetString());
    }

    [Theory]
    [InlineData("coordinator.example")]
    [InlineData("ftp://coordinator.example")]
    [InlineData("https://coordinator.example/pages")]
    [InlineData("https://coordinator.example/?a=b")]
    [InlineData("https://user:secret@coordinator.example")]
    public void An_Address_That_Is_No_Origin_Stops_The_Start_And_Is_Named(string value)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Coordinator:Authority"] = "http://idp.test",
            ["Coordinator:Ui:ClientSecret"] = "x",
            ["Coordinator:PublicUrl"] = value,
        }).Build();

        var failure = Assert.Throws<InvalidOperationException>(() => CoordinatorServerOptions.FromConfiguration(configuration));
        Assert.Contains("Coordinator:PublicUrl", failure.Message);
        Assert.DoesNotContain("secret", failure.Message);
    }
}
