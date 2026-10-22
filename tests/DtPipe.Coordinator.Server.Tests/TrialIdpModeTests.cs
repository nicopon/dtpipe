using System.Net;
using System.Text.Json;
using DtPipe.Coordinator.Server;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>The trial identity provider is opt-in by a long enough secret, and never beside an external provider.</summary>
public sealed class TrialIdpModeTests
{
    private const string Secret = "0123456789abcdef";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static CoordinatorServerOptions Options(params (string Key, string Value)[] values) => CoordinatorServerOptions.FromConfiguration(
        new ConfigurationBuilder().AddInMemoryCollection(values.ToDictionary(v => "Coordinator:" + v.Key, v => (string?)v.Value)).Build());

    [Fact]
    public void An_External_Authority_Beside_The_Trial_Password_Stops_The_Start()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Options(
            ("InsecureTrialIdp:Password", Secret), ("Authority", "https://idp.example"), ("PublicUrl", "https://c.example")));
        Assert.Contains("Coordinator:Authority", ex.Message);
    }

    [Fact]
    public void A_Password_Shorter_Than_Sixteen_Characters_Stops_The_Start()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Options(
            ("InsecureTrialIdp:Password", Secret[..15]), ("PublicUrl", "https://c.example")));
        Assert.Contains("16", ex.Message);
    }

    [Fact]
    public async Task Without_The_Password_The_Server_Is_No_Identity_Provider_And_Says_So_In_Its_Metadata()
    {
        using var host = new CoordinatorHost();
        var client = host.Anonymous();

        using var discovery = await client.GetAsync("/.well-known/openid-configuration", Ct);
        Assert.Equal(HttpStatusCode.NotFound, discovery.StatusCode);
        var metadata = JsonDocument.Parse(await client.GetStringAsync(CoordinatorApi.Metadata, Ct)).RootElement;
        Assert.False(metadata.TryGetProperty("dtpipe_trial_idp", out _));
    }
}
