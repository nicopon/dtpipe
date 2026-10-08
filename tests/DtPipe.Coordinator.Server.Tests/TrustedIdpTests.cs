using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// Who reaches what on the coordinator, over HTTP: a caller without a session, a page navigation, the
/// anonymous routes, and the two kinds of bearer token, each refused where the other belongs.
/// </summary>
public sealed class TrustedIdpTests : IDisposable
{
    private readonly CoordinatorHost _host = new();

    public void Dispose() => _host.Dispose();

    [Fact]
    public async Task An_Api_Call_Without_A_Session_Is_Told_401_And_Never_Redirected()
    {
        using var response = await _host.Anonymous().GetAsync("/api/me", TestContext.Current.CancellationToken);
        // The cookie handler names its login path in Location, but a 401 is never followed.
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task A_Page_Without_A_Session_Goes_To_The_Provider()
    {
        using var response = await _host.Anonymous().GetAsync("/", TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.StartsWith(CoordinatorHost.Issuer + "connect/authorize?", response.Headers.Location!.ToString());
    }

    [Fact]
    public async Task Healthz_Is_Anonymous()
    {
        using var response = await _host.Anonymous().GetAsync("/healthz", TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_Listed_Person_Bearing_A_Studio_Token_Reads_Their_Roles()
    {
        var me = await _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Designer)).GetFromJsonAsync<CoordinatorMe>("/api/me", Ct);
        Assert.Equal(CoordinatorHost.Designer, me!.Email);
        Assert.Equal(["designer", "reader"], me.Roles);
    }

    [Fact]
    public async Task An_Unlisted_Person_Bearing_A_Studio_Token_Is_Refused_With_403()
    {
        using var response = await _host.Bearer(CoordinatorHost.Person("zed@x.test")).GetAsync("/api/nodes", Ct);
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task A_Service_Token_Never_Reaches_Api()
    {
        using var response = await _host.Bearer(CoordinatorHost.Service("node-1")).GetAsync("/api/me", Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    public static TheoryData<string> DataPlane => ["/node-channel/negotiate?negotiateVersion=1", "/hub/command/negotiate?negotiateVersion=1"];

    [Theory]
    [MemberData(nameof(DataPlane))]
    public async Task A_Persons_Token_Never_Reaches_The_Data_Plane_And_A_Services_Does(string path)
    {
        var person = CoordinatorHost.Person(CoordinatorHost.Designer);
        using (var header = await _host.Bearer(person).PostAsync(path, null, Ct))
            Assert.Equal(HttpStatusCode.Unauthorized, header.StatusCode);
        using (var query = await _host.Anonymous().PostAsync(path + "&access_token=" + person, null, Ct))
            Assert.Equal(HttpStatusCode.Unauthorized, query.StatusCode);
        using (var service = await _host.Bearer(CoordinatorHost.Service("node-1")).PostAsync(path, null, Ct))
            Assert.Equal(HttpStatusCode.OK, service.StatusCode);
        Assert.Empty(_host.Services.GetRequiredService<EnrollmentQueue>().List());
    }

    [Fact]
    public async Task A_Persons_Token_Never_Reaches_A_Stream()
    {
        using var response = await _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Designer)).GetAsync("/api/stream/x/receive", Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task A_Persons_Token_From_Another_Client_Reaches_Nothing()
    {
        var other = CoordinatorHost.Person(CoordinatorHost.Designer, clientId: "coordinator-ui");
        using (var api = await _host.Bearer(other).GetAsync("/api/me", Ct))
            Assert.Equal(HttpStatusCode.Unauthorized, api.StatusCode);
        using (var channel = await _host.Bearer(other).PostAsync("/node-channel/negotiate?negotiateVersion=1", null, Ct))
            Assert.Equal(HttpStatusCode.Unauthorized, channel.StatusCode);
    }

    [Fact]
    public async Task A_Token_For_Another_Audience_Reaches_Nothing()
    {
        var foreign = CoordinatorHost.Person(CoordinatorHost.Designer, audience: null);
        using var response = await _host.Bearer(foreign).GetAsync("/api/me", Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Without_A_Studio_Client_No_Bearer_Reaches_Api()
    {
        using var host = new CoordinatorHost(studioClientId: null);
        using var response = await host.Bearer(CoordinatorHost.Person(CoordinatorHost.Designer)).GetAsync("/api/me", Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task A_Bearer_Changing_Something_Carries_The_Csrf_Header()
    {
        using var response = await _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Designer))
            .PutAsJsonAsync("/api/library/some-pipeline", new LibrarySave("a: {input: x}\n", null, null), Ct);
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task The_Metadata_Document_Is_Anonymous_And_Names_The_Provider_And_The_Contract()
    {
        using var response = await _host.Anonymous().GetAsync(CoordinatorApi.Metadata, Ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct)).RootElement;
        Assert.Equal("http://localhost", json.GetProperty("resource").GetString());
        Assert.Equal(CoordinatorHost.Issuer.TrimEnd('/'), json.GetProperty("authorization_servers")[0].GetString());
        Assert.Equal(CoordinatorApi.Contract, json.GetProperty("dtpipe_contract").GetInt32());
        Assert.Equal(CoordinatorHost.StudioClient, json.GetProperty("dtpipe_studio_client_id").GetString());

        using var host = new CoordinatorHost(studioClientId: null);
        var none = JsonDocument.Parse(await host.Anonymous().GetStringAsync(CoordinatorApi.Metadata, Ct)).RootElement;
        Assert.False(none.TryGetProperty("dtpipe_studio_client_id", out _));
    }

    [Fact]
    public async Task A_401_From_Api_Names_The_Metadata_Document()
    {
        const string metadata = "resource_metadata=\"http://localhost/.well-known/oauth-protected-resource\"";
        using (var anonymous = await _host.Anonymous().GetAsync("/api/me", Ct))
            Assert.Equal("Bearer " + metadata, anonymous.Headers.WwwAuthenticate.ToString());
        using var refused = await _host.Bearer(CoordinatorHost.Service("node-1")).GetAsync("/api/me", Ct);
        var challenge = refused.Headers.WwwAuthenticate.ToString();
        Assert.StartsWith("Bearer error=", challenge);
        Assert.EndsWith(", " + metadata, challenge);
    }
}
