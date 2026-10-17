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
            .PutAsJsonAsync("/api/library/some-pipeline", new LibrarySave("a: {input: x}\n", null), Ct);
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

    // ---- the third kind of caller: a monitor reads /health and nothing else ----

    private RightsStore Rights => _host.Services.GetRequiredService<RightsStore>();

    private void Provision()
    {
        Rights.AddIdentity(new Identity("watch", "supervisor", "", [], Kind: IdentityKinds.Monitor));
        Rights.AddIdentity(new Identity("node-1", "node-1", "", ["crm"]));
    }

    [Fact]
    public async Task A_Monitor_Reads_Health_With_The_Status_In_The_Body_And_A_Disarmed_Pipeline_Is_Critical()
    {
        Provision();
        var monitor = _host.Bearer(CoordinatorHost.Service("watch"));

        var all = await monitor.GetFromJsonAsync<JsonElement>("/health/pipelines", Ct);
        Assert.Equal("ok", all.GetProperty("status").GetString());
        Assert.Equal(0, all.GetProperty("pipelines").GetArrayLength());

        using var one = await monitor.GetAsync("/health/pipelines/ghost", Ct);
        Assert.Equal(HttpStatusCode.OK, one.StatusCode);
        Assert.Equal("no-store", one.Headers.CacheControl!.ToString());
        var body = JsonDocument.Parse(await one.Content.ReadAsStringAsync(Ct)).RootElement;
        Assert.Equal("critical", body.GetProperty("status").GetString());
        var pipeline = body.GetProperty("pipelines")[0];
        Assert.Equal(("ghost", "critical", "not-armed"),
            (pipeline.GetProperty("id").GetString(), pipeline.GetProperty("status").GetString(), pipeline.GetProperty("causes")[0].GetProperty("code").GetString()));
    }

    [Theory]
    [InlineData("/api/me")]
    [InlineData("/api/health")]
    [InlineData("/api/nodes")]
    public async Task A_Monitor_Never_Reaches_Api(string path)
    {
        Provision();
        using var response = await _host.Bearer(CoordinatorHost.Service("watch")).GetAsync(path, Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task A_Node_A_Stranger_And_A_Disabled_Monitor_Never_Reach_Health()
    {
        Provision();
        Rights.AddIdentity(new Identity("off", "sleeping", "", [], false, IdentityKinds.Monitor));

        foreach (var subject in new[] { "node-1", "stranger", "off" })
        {
            using var response = await _host.Bearer(CoordinatorHost.Service(subject)).GetAsync("/health/pipelines", Ct);
            Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        }
        Assert.Empty(_host.Services.GetRequiredService<EnrollmentQueue>().List());
    }

    [Fact]
    public async Task A_Person_And_An_Anonymous_Caller_Never_Reach_Health()
    {
        Provision();
        using (var person = await _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Admin)).GetAsync("/health/pipelines", Ct))
            Assert.Equal(HttpStatusCode.Unauthorized, person.StatusCode);
        using (var anonymous = await _host.Anonymous().GetAsync("/health/pipelines", Ct))
            Assert.Equal(HttpStatusCode.Unauthorized, anonymous.StatusCode);
    }

    [Fact]
    public async Task The_Pages_Read_The_Same_Health_As_A_Reader()
    {
        var reader = await _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Reader)).GetFromJsonAsync<JsonElement>("/api/health", Ct);
        Assert.Equal("ok", reader.GetProperty("status").GetString());
        using var unlisted = await _host.Bearer(CoordinatorHost.Person("zed@x.test")).GetAsync("/api/health", Ct);
        Assert.Equal(HttpStatusCode.Forbidden, unlisted.StatusCode);
    }

    [Fact]
    public async Task A_Monitors_Token_Reaches_The_Node_Channel_Only_To_Be_Refused_At_The_Announcement()
    {
        Provision();
        using var negotiated = await _host.Bearer(CoordinatorHost.Service("watch")).PostAsync("/node-channel/negotiate?negotiateVersion=1", null, Ct);

        // The transport opens for any service's token, as it does for a host no identity knows: what it may do there is the rights'
        // (NodeChannelHubTests), and a monitor is neither queued nor admitted to the data plane's groups.
        Assert.Equal(HttpStatusCode.OK, negotiated.StatusCode);
        Assert.Empty(_host.Services.GetRequiredService<EnrollmentQueue>().List());
        Assert.Empty(Rights.Find("watch")!.Groups);
    }
    // ---- the fourth kind of caller: a launcher starts the runs its identity lists, and reads them ----

    private void ProvisionLaunchers()
    {
        Provision();
        Rights.AddIdentity(new Identity("cron", "scheduler", "", [], Kind: IdentityKinds.Launcher, Pipelines: ["sales"]));
        Rights.AddIdentity(new Identity("sleeping", "sleeper", "", [], false, IdentityKinds.Launcher, ["sales"]));
    }

    private static StringContent Launch(string pipeline = "sales") =>
        new($$"""{"pipelineId":"{{pipeline}}"}""", System.Text.Encoding.UTF8, "application/json");

    [Fact]
    public async Task A_Launcher_Reaches_Its_Routes_And_Nothing_Else()
    {
        ProvisionLaunchers();
        var launcher = _host.Bearer(CoordinatorHost.Service("cron"));

        // It is admitted to both routes: what answers is the queue's refusal and the run's absence, not the authorization.
        using (var launched = await launcher.PostAsync("/service/runs", Launch(), Ct))
            Assert.Equal(HttpStatusCode.Conflict, launched.StatusCode);
        using (var read = await launcher.GetAsync("/service/runs/run-1", Ct))
            Assert.Equal(HttpStatusCode.NotFound, read.StatusCode);

        foreach (var path in new[] { "/api/me", "/api/health", "/api/runs" })
        {
            using var api = await launcher.GetAsync(path, Ct);
            Assert.Equal(HttpStatusCode.Unauthorized, api.StatusCode);
        }
        using (var runs = await launcher.PostAsync("/api/runs", Launch(), Ct))
            Assert.Equal(HttpStatusCode.Unauthorized, runs.StatusCode);
        using var health = await launcher.GetAsync("/health/pipelines", Ct);
        Assert.Equal(HttpStatusCode.Forbidden, health.StatusCode);
    }

    [Theory]
    [InlineData("node-1")]
    [InlineData("watch")]
    [InlineData("stranger")]
    [InlineData("sleeping")]
    public async Task A_Node_A_Monitor_A_Stranger_And_A_Disabled_Launcher_Never_Reach_The_Service_Routes(string subject)
    {
        ProvisionLaunchers();
        var caller = _host.Bearer(CoordinatorHost.Service(subject));

        using var launched = await caller.PostAsync("/service/runs", Launch(), Ct);
        using var read = await caller.GetAsync("/service/runs/run-1", Ct);

        Assert.Equal((HttpStatusCode.Forbidden, HttpStatusCode.Forbidden), (launched.StatusCode, read.StatusCode));
        Assert.Empty(_host.Services.GetRequiredService<EnrollmentQueue>().List());
    }

    [Fact]
    public async Task A_Person_A_Session_Cookie_And_An_Anonymous_Caller_Never_Reach_The_Service_Routes()
    {
        ProvisionLaunchers();
        using var cookie = new HttpRequestMessage(HttpMethod.Post, "/service/runs") { Content = Launch() };
        cookie.Headers.Add("Cookie", "dtpipe-coordinator=session");

        using var person = await _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Admin)).PostAsync("/service/runs", Launch(), Ct);
        using var anonymous = await _host.Anonymous().PostAsync("/service/runs", Launch(), Ct);
        using var session = await _host.Anonymous().SendAsync(cookie, Ct);
        using var read = await _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Admin)).GetAsync("/service/runs/run-1", Ct);

        Assert.All(new[] { person, anonymous, session, read }, r => Assert.Equal(HttpStatusCode.Unauthorized, r.StatusCode));
    }

    [Fact]
    public async Task A_Launchers_Token_Reaches_The_Node_Channel_Only_To_Be_Refused_At_The_Announcement()
    {
        ProvisionLaunchers();
        using var negotiated = await _host.Bearer(CoordinatorHost.Service("cron")).PostAsync("/node-channel/negotiate?negotiateVersion=1", null, Ct);

        Assert.Equal(HttpStatusCode.OK, negotiated.StatusCode);
        Assert.Empty(_host.Services.GetRequiredService<EnrollmentQueue>().List());
        Assert.Empty(Rights.Find("cron")!.Groups);
    }
}
