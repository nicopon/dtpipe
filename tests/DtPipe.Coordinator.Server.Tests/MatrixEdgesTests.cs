using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Nodes;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// Edges added beside the flow matrix: the ones a refused plan lacks, and the ones a node needs with the runners when it is added
/// or accepted. Nothing of the matrix is revoked, and each change is one audited write.
/// </summary>
public sealed class MatrixEdgesTests : IDisposable
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new();
    private readonly HttpClient _admin;

    public MatrixEdgesTests()
    {
        _admin = _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Admin));
        _admin.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
    }

    public void Dispose()
    {
        _admin.Dispose();
        _host.Dispose();
    }

    private RightsStore Rights => _host.Services.GetRequiredService<RightsStore>();

    /// <summary>A runner the inventory knows, whose identity puts it in <c>run</c>.</summary>
    private async Task ARunnerAsync()
    {
        using var created = await _admin.PostAsJsonAsync("/api/rights/identities", new { clientId = "r", name = "runner", groups = new[] { "run" } }, Web, Ct);
        created.EnsureSuccessStatusCode();
        var announcement = new NodeAnnouncement("runner", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid());
        _host.Services.GetRequiredService<ReplicaRegistry>().Announce("c-r", "r", announcement, ["run"], () => { });
        _host.Services.GetRequiredService<NodeInventory>().Announced("runner", "r", announcement, ["run"]);
    }

    [Fact]
    public async Task Allowing_Edges_Adds_Them_Beside_The_Matrix_And_Revokes_Nothing()
    {
        using var set = await _admin.PutAsJsonAsync("/api/rights/matrix", new { matrix = new Dictionary<string, string[]> { ["crm"] = ["run"], ["audit"] = ["run"] } }, Web, Ct);
        set.EnsureSuccessStatusCode();

        using var allowed = await _admin.PostAsJsonAsync("/api/rights/matrix/edges",
            new { edges = new[] { new { from = "crm", to = "run" }, new { from = "run", to = "crm" }, new { from = "crm", to = "audit" } } }, Web, Ct);
        using var none = await _admin.PostAsJsonAsync("/api/rights/matrix/edges", new { edges = Array.Empty<object>() }, Web, Ct);
        using var blank = await _admin.PostAsJsonAsync("/api/rights/matrix/edges", new { edges = new[] { new { from = "crm", to = " " } } }, Web, Ct);

        allowed.EnsureSuccessStatusCode();
        var rights = Rights.Snapshot();
        Assert.Equal(["audit", "run"], rights.Matrix["crm"]);
        Assert.Equal(["crm"], rights.Matrix["run"]);
        Assert.Equal(["run"], rights.Matrix["audit"]);
        Assert.Equal("Flow matrix: run → crm allowed, crm → audit allowed", rights.Audit![0].Change);
        Assert.Equal(CoordinatorHost.Admin, rights.Audit[0].By);
        Assert.Equal(HttpStatusCode.Conflict, none.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, blank.StatusCode);
    }

    [Fact]
    public async Task A_Node_Added_To_Exchange_With_The_Runners_Is_Given_Both_Edges_In_The_Same_Change()
    {
        using var alone = await _admin.PostAsJsonAsync("/api/rights/identities", new { clientId = "early", groups = new[] { "early" }, exchangeWithRunners = true }, Web, Ct);
        await ARunnerAsync();

        using var created = await _admin.PostAsJsonAsync("/api/rights/identities",
            new { clientId = "a", name = "node-a", groups = new[] { "crm" }, exchangeWithRunners = true }, Web, Ct);
        using var monitor = await _admin.PostAsJsonAsync("/api/rights/identities", new { clientId = "watch", kind = "monitor", exchangeWithRunners = true }, Web, Ct);

        created.EnsureSuccessStatusCode();
        var rights = Rights.Snapshot();
        Assert.Equal(["run"], rights.Matrix["crm"]);
        Assert.Equal(["crm"], rights.Matrix["run"]);
        Assert.Equal("Identity a added (node node-a, groups crm); flow matrix: crm → run allowed, run → crm allowed", rights.Audit![0].Change);
        Assert.Equal(HttpStatusCode.Conflict, alone.StatusCode);
        Assert.Contains("No runner is known", await alone.Content.ReadAsStringAsync(Ct));
        Assert.Null(Rights.Find("early"));
        Assert.Equal(HttpStatusCode.Conflict, monitor.StatusCode);
        Assert.Null(Rights.Find("watch"));
    }

    [Fact]
    public async Task A_Host_Accepted_To_Exchange_With_The_Runners_Is_Given_Both_Edges_In_The_Same_Change()
    {
        await ARunnerAsync();
        var enrollment = _host.Services.GetRequiredService<EnrollmentQueue>();
        enrollment.Propose("stranger", new NodeAnnouncement("proposed", "", [], NodeRole.Data, []), new Dictionary<string, string>());

        using var accepted = await _admin.PostAsJsonAsync("/api/rights/pending/stranger/accept",
            new { name = "node-s", groups = new[] { "node-s" }, exchangeWithRunners = true }, Web, Ct);

        accepted.EnsureSuccessStatusCode();
        var rights = Rights.Snapshot();
        Assert.Equal(["node-s"], Rights.Find("stranger")!.Groups);
        Assert.Equal(["run"], rights.Matrix["node-s"]);
        Assert.Equal(["node-s"], rights.Matrix["run"]);
        Assert.StartsWith("Identity stranger added (node node-s, groups node-s); flow matrix: ", rights.Audit![0].Change);
        Assert.Null(enrollment.Find("stranger"));
    }
}
