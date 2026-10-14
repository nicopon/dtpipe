using System.Net;
using System.Net.Http.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The routes <c>dtpipe studio</c> calls, through a person's bearer token: the catalog, what a job's
/// branches are, a brick's sample, and a save that says which version it expects.
/// </summary>
public sealed class StudioRoutesTests : IDisposable
{
    private const string Source = """{"input": "duck:shop.db", "provider-options": {"duck": {"query": "SELECT 1"}}}""";
    private readonly CoordinatorHost _host;
    private DesignCheck _verdict = new(true, null);
    private string? _checked;

    public StudioRoutesTests() => _host = new(check: yaml => { _checked = yaml; return _verdict; });

    public void Dispose() => _host.Dispose();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private HttpClient As(string email)
    {
        var client = _host.Bearer(CoordinatorHost.Person(email));
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        return client;
    }

    private NodeInventory Announce(bool online = true)
    {
        var inventory = _host.Services.GetRequiredService<NodeInventory>();
        var registry = _host.Services.GetRequiredService<ReplicaRegistry>();
        var announcement = new NodeAnnouncement("n1", "", [], NodeRole.Data,
            [new BrickInfo("orders", BrickKind.Source, "Orders", "every order", Source, [new BrickColumn("id", "BIGINT", false)], null)],
            DtpipeVersion: "9.9.9+abc");
        registry.Announce("c1", "n1", announcement, ["g"], () => { });
        inventory.Announced("n1", "n1", announcement, ["g"]);
        if (!online) registry.Disconnected("c1");
        return inventory;
    }

    [Fact]
    public async Task The_Catalog_Lists_Nodes_And_Bricks_With_Their_Declared_Branch()
    {
        Announce();
        var catalog = await As(CoordinatorHost.Reader).GetFromJsonAsync<BrickCatalogView>(CoordinatorApi.Catalog, Ct);
        Assert.Equal(new CatalogNode("n1", true, "data", "9.9.9+abc"), Assert.Single(catalog!.Nodes));
        var brick = Assert.Single(catalog.Bricks);
        Assert.Equal(("n1/orders", "source", "Orders"), (brick.Key, brick.Kind, brick.Title));
        Assert.Equal("duck:shop.db", brick.Branch["input"]!.GetValue<string>());
        Assert.Equal("id", Assert.Single(brick.Schema!).Name);
        Assert.Matches("^[0-9a-f]{12}$", brick.Version);
    }

    [Fact]
    public async Task Recognition_Names_The_Brick_A_Branch_Is_Or_Starts_With()
    {
        Announce();
        const string job = """
            whole:
              input: duck:shop.db
              provider-options:
                duck:
                  query: SELECT 1
            around:
              input: duck:shop.db
              provider-options:
                duck:
                  query: SELECT 1
              output: csv:out.csv
            other:
              input: duck:other.db
              output: csv:o.csv
            """;
        using var response = await As(CoordinatorHost.Reader).PostAsJsonAsync(CoordinatorApi.Recognize, new RecognizeRequest(job), Ct);
        var answer = await response.Content.ReadFromJsonAsync<RecognizeResponse>(Ct);
        Assert.Equal(("n1/orders", "n1/orders", (string?)null), (answer!.Branches["whole"].Match, answer.Branches["whole"].Source, answer.Branches["whole"].Sink));
        Assert.Equal(["n1"], answer.Branches["whole"].Nodes);
        Assert.Equal(new Recognition(null, "n1/orders", null), answer.Branches["around"]);
        Assert.Equal(new Recognition(null, null, null), answer.Branches["other"]);
    }

    [Fact]
    public async Task A_Job_That_Is_Not_Yaml_Is_Refused_With_400()
    {
        using var response = await As(CoordinatorHost.Reader).PostAsJsonAsync(CoordinatorApi.Recognize, new RecognizeRequest("- a list"), Ct);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task A_Sample_Is_A_Designers_And_Asks_A_Node_That_Is_Online()
    {
        Announce(online: false);
        using (var reader = await As(CoordinatorHost.Reader).PostAsync(CoordinatorApi.Sample("n1", "orders"), null, Ct))
            Assert.Equal(HttpStatusCode.Forbidden, reader.StatusCode);
        using var offline = await As(CoordinatorHost.Designer).PostAsync(CoordinatorApi.Sample("n1", "orders"), null, Ct);
        Assert.Equal(HttpStatusCode.Conflict, offline.StatusCode);
        Assert.Contains("offline", await offline.Content.ReadAsStringAsync(Ct));
    }

    private async Task<(HttpStatusCode Status, LibrarySaved? Saved, LibraryConflict? Conflict)> Save(string id, string yaml, string? ifMatch = null, bool create = false)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, CoordinatorApi.Library(id)) { Content = JsonContent.Create(new LibrarySave(yaml, "m")) };
        if (ifMatch is not null) request.Headers.TryAddWithoutValidation("If-Match", $"\"{ifMatch}\"");
        if (create) request.Headers.TryAddWithoutValidation("If-None-Match", "*");
        using var response = await As(CoordinatorHost.Designer).SendAsync(request, Ct);
        return response.StatusCode switch
        {
            HttpStatusCode.OK => (response.StatusCode, await response.Content.ReadFromJsonAsync<LibrarySaved>(Ct), null),
            HttpStatusCode.PreconditionFailed => (response.StatusCode, null, await response.Content.ReadFromJsonAsync<LibraryConflict>(Ct)),
            _ => (response.StatusCode, null, null),
        };
    }

    [Fact]
    public async Task A_Save_Creates_Only_What_Is_Absent_And_Replaces_Only_The_Version_It_Read()
    {
        var created = await Save("orders", "# One\na:\n  input: x\n", create: true);
        Assert.Equal(HttpStatusCode.OK, created.Status);
        var first = created.Saved!.Commit!;

        var again = await Save("orders", "# Two\na:\n  input: x\n", create: true);
        Assert.Equal((HttpStatusCode.PreconditionFailed, first), (again.Status, again.Conflict!.Commit));

        var stale = await Save("orders", "# Two\na:\n  input: x\n", ifMatch: "0000000");
        Assert.Equal((HttpStatusCode.PreconditionFailed, first), (stale.Status, stale.Conflict!.Commit));

        var replaced = await Save("orders", "# Two\na:\n  input: x\n", ifMatch: first);
        Assert.Equal(HttpStatusCode.OK, replaced.Status);
        Assert.NotEqual(first, replaced.Saved!.Commit);

        // Another pipeline commits; an unchanged save keeps this pipeline's own commit, which a later If-Match names.
        Assert.Equal(HttpStatusCode.OK, (await Save("other", "b:\n  input: y\n")).Status);
        var unchanged = await Save("orders", "# Two\na:\n  input: x\n", ifMatch: replaced.Saved.Commit);
        Assert.Equal((HttpStatusCode.OK, replaced.Saved.Commit), (unchanged.Status, unchanged.Saved!.Commit));
        Assert.Equal(HttpStatusCode.OK, (await Save("orders", "# Three\na:\n  input: x\n", ifMatch: unchanged.Saved.Commit)).Status);

        Assert.Equal(HttpStatusCode.PreconditionFailed, (await Save("missing", "a:\n  input: x\n", ifMatch: first)).Status);
    }

    [Fact]
    public async Task A_Save_Says_When_The_Saved_Plan_No_Longer_Matches()
    {
        var created = await Save("orders", "a:\n  input: x\n");
        Assert.False(created.Saved!.Redistribute);
        var plan = new DistributionPlan("plan-1", "orders", [], [], [], [], [], [], null);
        await _host.Services.GetRequiredService<PipelineLibrary>().SavePlanAsync("orders", plan, "plan", Ct);

        // The plan's commit is not an edit of the job: the version read before it still holds.
        var same = await Save("orders", "a:\n  input: x\n", ifMatch: created.Saved.Commit);
        Assert.False(same.Saved!.Redistribute);
        var changed = await Save("orders", "a:\n  input: z\n");
        Assert.True(changed.Saved!.Redistribute);
        Assert.False(changed.Saved.Deployed);
    }

    private Task<HttpResponseMessage> Put(HttpClient client, string id, string yaml)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, CoordinatorApi.Library(id)) { Content = JsonContent.Create(new LibrarySave(yaml, "m")) };
        return client.SendAsync(request, Ct);
    }

    [Fact]
    public async Task A_Save_The_Check_Refuses_Writes_And_Commits_Nothing()
    {
        var first = await Save("orders", "a:\n  input: x\n");
        _verdict = new DesignCheck(false, "'a': n1/orders does not accept what reaches it");

        using var refused = await Put(As(CoordinatorHost.Designer), "orders", "a:\n  input: z\n");
        Assert.Equal(HttpStatusCode.UnprocessableEntity, refused.StatusCode);
        var body = await refused.Content.ReadFromJsonAsync<LibraryRefused>(Ct);
        Assert.Equal("'a': n1/orders does not accept what reaches it", body!.Report);

        using var fresh = await Put(As(CoordinatorHost.Designer), "never", "a:\n  input: z\n");
        Assert.Equal(HttpStatusCode.UnprocessableEntity, fresh.StatusCode);

        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        var doc = await library.GetAsync("orders", Ct);
        Assert.Equal((first.Saved!.Commit, "a:\n  input: x\n"), (doc!.Commit, doc.Yaml));
        Assert.Null(await library.GetAsync("never", Ct));
        Assert.Equal("a:\n  input: z\n", _checked);
    }

    [Fact]
    public async Task The_Check_Reads_The_Job_As_It_Was_Sent()
    {
        const string raw = "# A comment\na:   {input:   x}\n";
        Assert.Equal(HttpStatusCode.OK, (await Save("orders", raw)).Status);
        Assert.Equal(raw, _checked);
        Assert.Equal(raw, (await _host.Services.GetRequiredService<PipelineLibrary>().GetAsync("orders", Ct))!.Yaml);
    }

    [Fact]
    public async Task Without_A_Runner_The_Real_Check_Refuses_And_Names_Why()
    {
        using var host = new CoordinatorHost();
        using var client = host.Bearer(CoordinatorHost.Person(CoordinatorHost.Designer));
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        using var response = await Put(client, "orders", "a:\n  input: x\n");
        Assert.Equal(HttpStatusCode.UnprocessableEntity, response.StatusCode);
        Assert.Contains("No runner is online", (await response.Content.ReadFromJsonAsync<LibraryRefused>(Ct))!.Report);
        Assert.Null(await host.Services.GetRequiredService<PipelineLibrary>().GetAsync("orders", Ct));
    }

    [Fact]
    public async Task The_Library_Is_Listed_And_Read_At_The_Version_Of_Its_Job()
    {
        var created = await Save("orders", "a:\n  input: x\n");
        var plan = new DistributionPlan("plan-1", "orders", [], [], [], [], [], [], null);
        await _host.Services.GetRequiredService<PipelineLibrary>().SavePlanAsync("orders", plan, "plan", Ct);

        var reader = As(CoordinatorHost.Reader);
        var listing = await reader.GetFromJsonAsync<LibraryItem[]>(CoordinatorApi.LibraryList, Ct);
        Assert.Equal(("orders", created.Saved!.Commit), (Assert.Single(listing!).Id, listing![0].Commit));

        // The plan saved beside the job is a commit of the folder, not an edit: the job's version stays.
        var job = await reader.GetFromJsonAsync<LibraryJob>(CoordinatorApi.Library("orders"), Ct);
        Assert.Equal(new LibraryJob("orders", "a:\n  input: x\n", created.Saved.Commit), job);

        using var missing = await reader.GetAsync(CoordinatorApi.Library("nothing"), Ct);
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
    }
}

