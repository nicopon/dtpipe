using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What the routes answer when a store the coordinator keeps its state in is down, over an armed deployment the host read back at
/// start: a change that needs the store is a 503 and changes nothing, and what it kept is still read.
/// </summary>
public sealed class DeploymentRoutesTests : IDisposable
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new();
    private readonly HttpClient _admin;

    public DeploymentRoutesTests()
    {
        // The host reads its armed deployments when it is built, so the store holds one before the first request.
        var armed = new ArmedDeployment("sales", "0123456789abcdef", "job", null, new Dictionary<string, Binding>(), DateTimeOffset.UtcNow, "ann@x.test");
        MemoryDeploymentStore.At(_host.StateDirectory).Save("sales", JsonSerializer.Serialize(armed, Web));
        _admin = _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Admin));
        _admin.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
    }

    public void Dispose()
    {
        _admin.Dispose();
        _host.Dispose();
    }

    [Fact]
    public async Task A_Store_That_Is_Down_Answers_503_To_An_Undeploy_And_The_Deployment_Stays()
    {
        _host.Deployments.Down = true;

        using var refused = await _admin.DeleteAsync("/api/deployments/sales", Ct);
        using var listed = await _admin.GetAsync("/api/deployments", Ct);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
        Assert.Contains("\"error\"", await refused.Content.ReadAsStringAsync(Ct));
        Assert.Equal("sales", (await listed.Content.ReadFromJsonAsync<JsonElement>(Web, Ct))[0].GetProperty("pipelineId").GetString());
        _host.Deployments.Down = false;
        using var done = await _admin.DeleteAsync("/api/deployments/sales", Ct);
        Assert.Equal(HttpStatusCode.Accepted, done.StatusCode);
        Assert.Empty(MemoryDeploymentStore.At(_host.StateDirectory).Pipelines());
    }

    [Fact]
    public async Task A_Run_Journal_That_Is_Down_Answers_503_To_A_Launch_And_Queues_Nothing()
    {
        _host.Runs.Down = true;

        using var refused = await _admin.PostAsJsonAsync("/api/runs", new { pipelineId = "sales" }, Web, Ct);
        using var queue = await _admin.GetAsync("/api/runs/queue", Ct);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
        Assert.Equal("[]", await queue.Content.ReadAsStringAsync(Ct));
    }
}
