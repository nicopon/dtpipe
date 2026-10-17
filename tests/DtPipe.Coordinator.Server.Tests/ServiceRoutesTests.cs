using System.Net;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What <c>/service</c> answers a launcher, in the order it decides: what is malformed, what the identity may name, then the queue's
/// own refusals. The runs themselves (the key, the author) are <see cref="RunQueueTests"/>'.
/// </summary>
public sealed class ServiceRoutesTests : IDisposable
{
    private readonly CoordinatorHost _host = new();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public void Dispose() => _host.Dispose();

    private HttpClient Launcher()
    {
        var rights = _host.Services.GetRequiredService<RightsStore>();
        rights.AddIdentity(new Identity("cron", "scheduler", "", [], Kind: IdentityKinds.Launcher, Pipelines: ["sales"]));
        return _host.Bearer(CoordinatorHost.Service("cron"));
    }

    private static StringContent Json(string body) => new(body, Encoding.UTF8, "application/json");

    private static HttpRequestMessage Post(string body, string? key = null)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/service/runs") { Content = Json(body) };
        if (key is not null) request.Headers.TryAddWithoutValidation("Idempotency-Key", key);
        return request;
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("""{"pipelineId":""}""")]
    [InlineData("""{"pipelineId":"  "}""")]
    public async Task A_Request_That_Names_No_Pipeline_Is_Refused_And_Never_Launches_The_Last_One_Deployed(string body)
    {
        using var response = await Launcher().SendAsync(Post(body), Ct);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task A_Request_With_No_Body_Is_Refused()
    {
        using var response = await Launcher().PostAsync("/service/runs", null, Ct);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Theory]
    [InlineData("two words")]
    [InlineData("semi;colon")]
    public async Task A_Key_Out_Of_Bounds_Is_Refused(string key)
    {
        using var response = await Launcher().SendAsync(Post("""{"pipelineId":"sales"}""", key), Ct);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task A_Key_Longer_Than_The_Bound_Is_Refused()
    {
        using var response = await Launcher().SendAsync(Post("""{"pipelineId":"sales"}""", new string('k', 129)), Ct);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task A_Pipeline_Off_The_List_Is_Refused_Whether_Or_Not_It_Exists()
    {
        var launcher = Launcher();

        using var known = await launcher.SendAsync(Post("""{"pipelineId":"crm"}"""), Ct);
        using var ghost = await launcher.SendAsync(Post("""{"pipelineId":"ghost"}"""), Ct);

        Assert.Equal((HttpStatusCode.Forbidden, HttpStatusCode.Forbidden), (known.StatusCode, ghost.StatusCode));
        Assert.Equal(await known.Content.ReadAsStringAsync(Ct), (await ghost.Content.ReadAsStringAsync(Ct)).Replace("ghost", "crm"));
    }

    [Fact]
    public async Task A_Listed_Pipeline_That_Is_Not_Armed_Is_A_Conflict_And_Nothing_Is_Deployed()
    {
        using var response = await Launcher().SendAsync(Post("""{"pipelineId":"sales"}""", "night-1"), Ct);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Contains("not deployed", await response.Content.ReadAsStringAsync(Ct));
        Assert.Empty(_host.Services.GetRequiredService<DeploymentManager>().Queue);
    }

    [Fact]
    public async Task A_Run_Off_The_List_Is_Told_Apart_From_No_Run_By_Nothing()
    {
        using var response = await Launcher().GetAsync("/service/runs/run-1", Ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}
