using System.Net;
using System.Net.Http.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The routes <c>dtpipe studio</c> calls, through a person's bearer token: the catalog, what a job's
/// branches are, a brick's sample, and a save that says which version it expects.
/// </summary>
public sealed class StudioRoutesTests : IDisposable
{
    private const string Source = """{"input": "duck:shop.db", "provider-options": {"duck": {"query": "SELECT 1"}}}""";
    private const string Sink = """{"output": "csv:out.csv"}""";
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
            [TestBricks.Announced("orders", BrickKind.Source, Source, "Orders", "every order"), TestBricks.Announced("archive", BrickKind.Sink, Sink, "Archive")],
            DtpipeVersion: "9.9.9+abc");
        registry.Announce("c1", "n1", announcement, ["g"], () => { });
        inventory.Announced("n1", "n1", announcement, ["g"]);
        // The rights say who a node is: a host that announced itself has an identity there.
        var rights = _host.Services.GetRequiredService<RightsStore>();
        if (rights.Find("n1") is null) rights.AddIdentity(new Identity("n1", "n1", "", ["g"]));
        if (!online) registry.Disconnected("c1");
        return inventory;
    }

    [Fact]
    public async Task The_Catalog_Lists_Nodes_And_Bricks_By_The_Reference_A_Job_Makes_To_Them_And_Never_Their_Text()
    {
        Announce();
        var catalog = await As(CoordinatorHost.Reader).GetFromJsonAsync<BrickCatalogView>(CoordinatorApi.Catalog, Ct);
        Assert.Equal(new CatalogNode("n1", true, "data", "9.9.9+abc"), Assert.Single(catalog!.Nodes));
        var source = catalog.Bricks.Single(b => b.Id == "orders");
        var sink = catalog.Bricks.Single(b => b.Id == "archive");
        Assert.Equal(("n1/orders", "source", "Orders"), (source.Key, source.Kind, source.Title));
        Assert.Equal("""{"input":"brick:n1/orders"}""", source.Branch.ToJsonString());
        Assert.Equal("""{"output":"brick:n1/archive"}""", sink.Branch.ToJsonString());
        Assert.Matches("^[0-9a-f]{12}$", source.Version);
    }

    [Fact]
    public async Task Recognition_Names_The_Brick_A_Reference_Makes_In_The_Direction_It_Makes_It()
    {
        Announce();
        const string job = """
            read:
              input: brick:n1/orders
            write:
              from: read
              output: brick:n1/archive
            backwards:
              input: brick:n1/archive
            ghost:
              input: brick:n1/nothing
            says_more:
              input: brick:n1/orders
              output: csv:out.csv
            text:
              input: duck:shop.db
              provider-options:
                duck:
                  query: SELECT 1
              output: csv:o.csv
            """;
        using var response = await As(CoordinatorHost.Reader).PostAsJsonAsync(CoordinatorApi.Recognize, new RecognizeRequest(job), Ct);
        var answer = await response.Content.ReadFromJsonAsync<RecognizeResponse>(Ct);
        Assert.Equal(new[] { "n1/orders", "n1/archive" }, new[] { answer!.Branches["read"].Match, answer.Branches["write"].Match });
        foreach (var alias in new[] { "backwards", "ghost", "says_more", "text" })
            Assert.Equal(new Recognition(null), answer.Branches[alias]);
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
    public async Task A_Save_Says_Nothing_Is_Deployed_For_A_Pipeline_That_Is_Not_Armed()
    {
        var created = await Save("orders", "a:\n  input: x\n");
        var changed = await Save("orders", "a:\n  input: z\n");

        Assert.False(created.Saved!.Deployed);
        Assert.False(changed.Saved!.Deployed);
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

    private const string Orders = "a:\n  input: brick:n1/orders\n";

    [Fact]
    public async Task A_Save_Binds_The_Branches_It_Names_And_The_Reference_Names_Their_Bricks()
    {
        Announce();
        var version = TestBricks.VersionOf(BrickKind.Source, Source);
        _verdict = Pinned(version, null);
        using var request = new HttpRequestMessage(HttpMethod.Put, CoordinatorApi.Library("orders"))
        {
            Content = JsonContent.Create(new LibrarySave(Orders, "m", new Dictionary<string, Binding> { ["a"] = new Binding("n1") })),
        };
        using var saved = await As(CoordinatorHost.Designer).SendAsync(request, Ct);
        Assert.Equal(HttpStatusCode.OK, saved.StatusCode);

        var doc = await _host.Services.GetRequiredService<PipelineLibrary>().GetAsync("orders", Ct);
        Assert.Equal(new Binding("n1", "orders", "strict", new BindingPin(version)), doc!.Bindings["a"]);
        Assert.Empty(doc.BindingProblems);
    }

    [Fact]
    public async Task A_Mode_That_Is_None_Of_The_Four_Is_Refused_And_Nothing_Is_Written()
    {
        Announce();
        using var request = new HttpRequestMessage(HttpMethod.Put, CoordinatorApi.Library("orders"))
        {
            Content = JsonContent.Create(new LibrarySave(Orders, "m", new Dictionary<string, Binding> { ["a"] = new Binding("n1", "orders", "loose") })),
        };
        using var refused = await As(CoordinatorHost.Designer).SendAsync(request, Ct);
        Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        Assert.Contains("loose", await refused.Content.ReadAsStringAsync(Ct));

        using var bound = await As(CoordinatorHost.Designer).PutAsJsonAsync(
            "/api/library/orders/bindings", new { bindings = new Dictionary<string, Binding> { ["a"] = new Binding("n1", "orders", "sloppy") } }, Ct);
        Assert.Equal(HttpStatusCode.BadRequest, bound.StatusCode);
        Assert.Contains("sloppy", await bound.Content.ReadAsStringAsync(Ct));
        Assert.Null(await _host.Services.GetRequiredService<PipelineLibrary>().GetAsync("orders", Ct));
    }

    private static DesignCheck Pinned(string version, string? contract) =>
        new(true, null, new Dictionary<string, CheckedPin> { ["a"] = new CheckedPin("orders", new BindingPin(version, contract)) });

    private async Task<HttpStatusCode> Put(Binding binding)
    {
        using var request = new HttpRequestMessage(HttpMethod.Put, CoordinatorApi.Library("orders"))
        {
            Content = JsonContent.Create(new LibrarySave(Orders, "m", new Dictionary<string, Binding> { ["a"] = binding })),
        };
        using var response = await As(CoordinatorHost.Designer).SendAsync(request, Ct);
        return response.StatusCode;
    }

    private async Task<Binding> HeldAsync() =>
        (await _host.Services.GetRequiredService<PipelineLibrary>().GetAsync("orders", Ct))!.Bindings["a"];

    [Fact]
    public async Task A_Pin_Is_Written_By_The_Check_Never_By_The_Request()
    {
        Announce();
        _verdict = Pinned("v1", "{\"hash\":\"checked\"}");

        Assert.Equal(HttpStatusCode.OK, await Put(new Binding("n1", "orders", "contract", new BindingPin("forged", "{\"hash\":\"forged\"}"))));

        var held = await HeldAsync();
        Assert.Equal(new Binding("n1", "orders", "contract", new BindingPin("v1", "{\"hash\":\"checked\"}")), held);
        // The check judged the binding without the pin the request brought.
        Assert.Null(_host.CheckedBindings!["a"].Pin);
    }

    [Fact]
    public async Task A_Binding_The_Check_Gave_No_Contract_Stays_Unpinned_And_So_Unchecked()
    {
        Announce();
        _verdict = Pinned("v1", null);

        Assert.Equal(HttpStatusCode.OK, await Put(new Binding("n1", "orders", "compatible")));

        var doc = (await _host.Services.GetRequiredService<PipelineLibrary>().GetAsync("orders", Ct))!;
        Assert.Null(doc.Bindings["a"].Pin);
        Assert.Equal(BindingCauses.Unchecked, Assert.Single(doc.BindingProblems).Cause);
    }

    [Fact]
    public async Task A_Strict_Reference_Is_Pinned_At_The_Version_The_Check_Saw_And_Never_At_Its_Contract()
    {
        Announce();
        _verdict = Pinned("v1", "{}");

        Assert.Equal(HttpStatusCode.OK, await Put(new Binding("n1", "orders", "strict", new BindingPin("forged"))));

        Assert.Equal(new BindingPin("v1"), (await HeldAsync()).Pin);
    }

    [Fact]
    public async Task A_Save_That_Names_No_Mode_Keeps_The_One_The_Library_Holds_And_Pins_It_Again()
    {
        Announce();
        _verdict = Pinned("v1", "{}");
        Assert.Equal(HttpStatusCode.OK, await Put(new Binding("n1", "orders", "tolerant")));
        _verdict = Pinned("v2", "{}");

        // A studio knows no mode: it publishes the node alone.
        Assert.Equal(HttpStatusCode.OK, await Put(new Binding("n1")));

        var held = await HeldAsync();
        Assert.Equal(("tolerant", "orders", "v2"), (held.Mode, held.Brick, held.Pin!.Version));
        Assert.Null(held.Pin.Contract);
    }

    [Fact]
    public async Task A_Save_Pins_Again_The_Bindings_It_Does_Not_Name_That_Are_Not_Strict()
    {
        Announce();
        _verdict = Pinned("v1", "{}");
        Assert.Equal(HttpStatusCode.OK, await Put(new Binding("n1", "orders", "contract")));
        _verdict = Pinned("v2", "{\"hash\":\"later\"}");

        // The new text names no binding: the one kept is still checked, so it is pinned again.
        using var request = new HttpRequestMessage(HttpMethod.Put, CoordinatorApi.Library("orders")) { Content = JsonContent.Create(new LibrarySave(Orders, "again")) };
        using var response = await As(CoordinatorHost.Designer).SendAsync(request, Ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        Assert.Equal(new BindingPin("v2", "{\"hash\":\"later\"}"), (await HeldAsync()).Pin);
    }

    [Fact]
    public async Task New_Bindings_Go_Through_The_Check_Like_A_Save_And_A_Refused_One_Writes_Nothing()
    {
        Announce();
        Assert.Equal(HttpStatusCode.OK, (await Save("orders", Orders)).Status);
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        _verdict = new DesignCheck(false, "'a': n1/orders does not accept what reaches it");

        using var refused = await As(CoordinatorHost.Designer).PutAsJsonAsync(
            "/api/library/orders/bindings", new { bindings = new Dictionary<string, Binding> { ["a"] = new Binding("n1") } }, Ct);

        Assert.Equal(HttpStatusCode.UnprocessableEntity, refused.StatusCode);
        Assert.Equal("'a': n1/orders does not accept what reaches it", (await refused.Content.ReadFromJsonAsync<LibraryRefused>(Ct))!.Report);
        Assert.Equal(new Binding("n1", "orders"), (await library.GetAsync("orders", Ct))!.Bindings["a"]);
        Assert.Equal(Orders, _checked);

        _verdict = new DesignCheck(true, null);
        using var accepted = await As(CoordinatorHost.Designer).PutAsJsonAsync(
            "/api/library/orders/bindings", new { bindings = new Dictionary<string, Binding> { ["a"] = new Binding("n1") } }, Ct);
        Assert.Equal(HttpStatusCode.OK, accepted.StatusCode);
        Assert.Equal("orders", (await library.GetAsync("orders", Ct))!.Bindings["a"].Brick);
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
        await _host.Services.GetRequiredService<PipelineLibrary>().SaveBindingsAsync(
            "orders", new Dictionary<string, Binding> { ["a"] = new Binding("node-a") }, "bind", Ct);

        var reader = As(CoordinatorHost.Reader);
        var listing = await reader.GetFromJsonAsync<LibraryItem[]>(CoordinatorApi.LibraryList, Ct);
        Assert.Equal(("orders", created.Saved!.Commit), (Assert.Single(listing!).Id, listing![0].Commit));

        // The bindings saved beside the job are a commit of the folder, not an edit: the job's version stays.
        var job = await reader.GetFromJsonAsync<LibraryJob>(CoordinatorApi.Library("orders"), Ct);
        Assert.Equal(new LibraryJob("orders", "a:\n  input: x\n", created.Saved.Commit), job);

        using var missing = await reader.GetAsync(CoordinatorApi.Library("nothing"), Ct);
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
    }
}

