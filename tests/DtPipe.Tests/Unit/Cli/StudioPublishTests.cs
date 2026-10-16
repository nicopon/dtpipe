using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Publishing a job from the studio into the coordinator's library: the job as saved in the folder and
/// nothing beside it, created only while absent, replacing only the version published last from here
/// or the one the person chose after a conflict.
/// </summary>
public sealed class StudioPublishTests : IAsyncLifetime
{
    private const string Witness = "witness-launch-value-Q4z";
    private readonly string _dir = Directory.CreateTempSubdirectory("studio-publish-").FullName;
    private readonly StudioFakes _fakes = new();
    private readonly HttpClient _browser = new(new HttpClientHandler { AllowAutoRedirect = false });
    private CoordinatorLink _link = null!;
    private StudioServer _studio = null!;

    public async ValueTask InitializeAsync()
    {
        await _fakes.StartAsync();
        _link = new CoordinatorLink(new Uri(_fakes.CoordinatorUrl));
        _studio = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            new FakeStudioEngine(), new DesignService(IBrickLookup.None), [], [], new InMemorySecretsManager(), _link,
            lookup => new DesignService(lookup)), CancellationToken.None);
        File.WriteAllText(Path.Combine(_dir, "orders.yaml"), "# Orders\nmain:\n  input: duck:${{DB}}\n  output: csv:o.csv\n");
    }

    public async ValueTask DisposeAsync()
    {
        await _studio.DisposeAsync();
        _link.Dispose();
        await _fakes.DisposeAsync();
        _browser.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<(HttpStatusCode Status, JsonElement Body)> CallAsync(HttpMethod method, string path, object? body = null)
    {
        using var request = new HttpRequestMessage(method, $"http://127.0.0.1:{_studio.Port}{path}");
        request.Headers.Add(StudioServer.TokenHeader, _studio.Token);
        if (body is not null) request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var response = await _browser.SendAsync(request, Ct);
        var text = await response.Content.ReadAsStringAsync(Ct);
        return (response.StatusCode, text.Length == 0 ? default : JsonDocument.Parse(text).RootElement.Clone());
    }

    private async Task SignInAsync()
    {
        var (_, login) = await CallAsync(HttpMethod.Post, "/api/coordinator/login", new { });
        using var authorize = await _browser.GetAsync(login.GetProperty("url").GetString(), Ct);
        using var callback = await _browser.GetAsync(authorize.Headers.Location, Ct);
        Assert.Equal(HttpStatusCode.OK, callback.StatusCode);
    }

    private async Task<string> HashAsync() => (await CallAsync(HttpMethod.Get, "/api/job?path=orders.yaml")).Body.GetProperty("hash").GetString()!;

    private Task<(HttpStatusCode Status, JsonElement Body)> PublishAsync(string hash, string? replace = null) =>
        CallAsync(HttpMethod.Post, "/api/publish", new { path = "orders.yaml", hash, id = "orders", message = "from the studio", replace });

    [Fact]
    public async Task The_saved_job_is_sent_without_its_launch_values_or_its_layout()
    {
        await SignInAsync();
        await CallAsync(HttpMethod.Put, "/api/launch", new { path = "orders.yaml", overrides = new { DB = new { value = Witness } } });

        var (status, saved) = await PublishAsync(await HashAsync());

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("orders", saved.GetProperty("id").GetString());
        Assert.DoesNotContain(Witness, _fakes.LastSaveBody);
        var body = JsonDocument.Parse(_fakes.LastSaveBody!).RootElement;
        Assert.Equal(File.ReadAllText(Path.Combine(_dir, "orders.yaml")), body.GetProperty("yaml").GetString());
        Assert.False(body.TryGetProperty("layout", out _));
    }

    [Fact]
    public async Task A_job_not_saved_as_the_page_has_it_is_refused()
    {
        await SignInAsync();
        var hash = await HashAsync();
        File.AppendAllText(Path.Combine(_dir, "orders.yaml"), "# changed on disk\n");

        var (status, answer) = await PublishAsync(hash);

        Assert.Equal(HttpStatusCode.Conflict, status);
        Assert.Contains("save it", answer.GetProperty("error").GetString());
        Assert.Empty(_fakes.Library);
    }

    [Fact]
    public async Task A_publication_creates_then_replaces_only_its_own_version_and_a_conflict_is_replaced_only_when_asked()
    {
        await SignInAsync();
        var (created, first) = await PublishAsync(await HashAsync());
        Assert.Equal(HttpStatusCode.OK, created);

        File.AppendAllText(Path.Combine(_dir, "orders.yaml"), "# second\n");
        var (replaced, second) = await PublishAsync(await HashAsync());
        Assert.Equal(HttpStatusCode.OK, replaced);
        Assert.NotEqual(first.GetProperty("commit").GetString(), second.GetProperty("commit").GetString());

        // Someone else saves the pipeline in between: this studio's version is no longer the one in place.
        _fakes.Library["orders"] = ("# theirs\n", "commit-theirs");
        var (conflict, refused) = await PublishAsync(await HashAsync());
        Assert.Equal(HttpStatusCode.PreconditionFailed, conflict);
        Assert.Equal("commit-theirs", refused.GetProperty("commit").GetString());
        Assert.Equal("# theirs\n", _fakes.Library["orders"].Yaml);

        var (forced, _) = await PublishAsync(await HashAsync(), replace: "commit-theirs");
        Assert.Equal(HttpStatusCode.OK, forced);
        Assert.Contains("# second", _fakes.Library["orders"].Yaml);
    }

    [Fact]
    public async Task An_id_taken_before_a_first_publication_is_a_conflict()
    {
        await SignInAsync();
        _fakes.Library["orders"] = ("# theirs\n", "commit-theirs");

        var (status, refused) = await PublishAsync(await HashAsync());

        Assert.Equal(HttpStatusCode.PreconditionFailed, status);
        Assert.Equal("commit-theirs", refused.GetProperty("commit").GetString());
    }

    [Fact]
    public async Task A_publication_says_when_its_plan_must_be_redone()
    {
        await SignInAsync();
        _fakes.Redistribute = true;

        var (_, saved) = await PublishAsync(await HashAsync());

        Assert.True(saved.GetProperty("redistribute").GetBoolean());
        var (_, again) = await CallAsync(HttpMethod.Get, "/api/publish?path=orders.yaml");
        Assert.Equal("orders", again.GetProperty("publication").GetProperty("id").GetString());
    }

    [Fact]
    public async Task An_id_that_is_no_pipeline_id_is_refused()
    {
        await SignInAsync();
        var (status, _) = await CallAsync(HttpMethod.Post, "/api/publish", new { path = "orders.yaml", hash = await HashAsync(), id = "../Orders" });
        Assert.Equal(HttpStatusCode.BadRequest, status);
    }

    [Fact]
    public async Task A_refused_publication_shows_the_report_of_the_coordinators_check()
    {
        await SignInAsync();
        _fakes.Refusal = new LibraryRefused("The designer's check refused this job: nothing was saved.", "'main': n1/archive does not accept what reaches it");

        var (status, answer) = await PublishAsync(await HashAsync());

        Assert.Equal(HttpStatusCode.UnprocessableEntity, status);
        Assert.Equal("'main': n1/archive does not accept what reaches it", answer.GetProperty("report").GetString());
        Assert.Empty(_fakes.Library);
        var (_, kept) = await CallAsync(HttpMethod.Get, "/api/publish?path=orders.yaml");
        Assert.Equal(JsonValueKind.Null, kept.GetProperty("publication").ValueKind);
    }

    [Fact]
    public async Task A_library_pipeline_opens_as_a_new_file_and_the_next_publication_replaces_that_version()
    {
        _fakes.Library["customers"] = ("# Customers\nmain:\n  input: duck:${{DB}}\n  output: csv:c.csv\n", "commit-theirs");
        var (signedOut, _) = await CallAsync(HttpMethod.Get, "/api/library");
        Assert.Equal(HttpStatusCode.Conflict, signedOut);
        await SignInAsync();

        var (listed, listing) = await CallAsync(HttpMethod.Get, "/api/library");
        Assert.Equal(HttpStatusCode.OK, listed);
        Assert.Equal("customers", Assert.Single(listing.GetProperty("pipelines").EnumerateArray()).GetProperty("id").GetString());

        var (opened, open) = await CallAsync(HttpMethod.Post, "/api/library/open", new { id = "customers" });
        Assert.Equal(HttpStatusCode.OK, opened);
        Assert.Equal("customers.yaml", open.GetProperty("path").GetString());
        Assert.Equal(_fakes.Library["customers"].Yaml, File.ReadAllText(Path.Combine(_dir, "customers.yaml")));

        // The file is never overwritten, even by the same pipeline opened twice.
        File.AppendAllText(Path.Combine(_dir, "customers.yaml"), "# mine\n");
        var (again, refused) = await CallAsync(HttpMethod.Post, "/api/library/open", new { id = "customers" });
        Assert.Equal(HttpStatusCode.Conflict, again);
        Assert.Contains("already exists", refused.GetProperty("error").GetString());
        Assert.Contains("# mine", File.ReadAllText(Path.Combine(_dir, "customers.yaml")));

        // The version read was kept: the publication sends If-Match for it, and the library accepts it.
        var hash = (await CallAsync(HttpMethod.Get, "/api/job?path=customers.yaml")).Body.GetProperty("hash").GetString();
        var (published, saved) = await CallAsync(HttpMethod.Post, "/api/publish", new { path = "customers.yaml", hash, id = "customers", message = "m" });
        Assert.Equal(HttpStatusCode.OK, published);
        Assert.NotEqual("commit-theirs", saved.GetProperty("commit").GetString());
        Assert.Contains("# mine", _fakes.Library["customers"].Yaml);
    }

    [Fact]
    public async Task A_pipeline_the_library_does_not_hold_or_an_id_that_is_none_is_not_opened()
    {
        await SignInAsync();
        var (missing, _) = await CallAsync(HttpMethod.Post, "/api/library/open", new { id = "nothing" });
        Assert.Equal(HttpStatusCode.NotFound, missing);
        var (bad, _) = await CallAsync(HttpMethod.Post, "/api/library/open", new { id = "../orders" });
        Assert.Equal(HttpStatusCode.BadRequest, bad);
        Assert.False(File.Exists(Path.Combine(_dir, "nothing.yaml")));
    }

    private LibrarySave LastSave() => JsonSerializer.Deserialize<LibrarySave>(_fakes.LastSaveBody!, JsonSerializerOptions.Web)!;

    [Fact]
    public async Task A_brick_only_one_node_offers_is_published_with_its_node()
    {
        await SignInAsync();
        _fakes.Answers["main"] = new Recognition("n1/orders", null, null, ["n1"]);

        var (status, _) = await PublishAsync(await HashAsync());

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("n1", LastSave().Placement!["main"]);
    }

    [Fact]
    public async Task A_brick_two_nodes_offer_is_published_unnamed()
    {
        await SignInAsync();
        _fakes.Answers["main"] = new Recognition("n1/orders", null, null, ["n1", "n2"]);

        var (status, _) = await PublishAsync(await HashAsync());

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Empty(LastSave().Placement ?? new Dictionary<string, string>());
    }

    [Fact]
    public async Task The_page_lists_the_nodes_that_offer_each_brick_branch()
    {
        await SignInAsync();
        _fakes.Answers["main"] = new Recognition("n1/orders", null, null, ["n1", "n2"]);
        _fakes.Answers["other"] = new Recognition(null, null, null);

        var (status, body) = await CallAsync(HttpMethod.Get, "/api/placement?path=orders.yaml");

        Assert.Equal(HttpStatusCode.OK, status);
        var branch = Assert.Single(body.GetProperty("branches").EnumerateArray());
        Assert.Equal("main", branch.GetProperty("alias").GetString());
        Assert.Equal(["n1", "n2"], branch.GetProperty("nodes").EnumerateArray().Select(n => n.GetString()));
    }

    [Fact]
    public async Task A_node_the_person_chose_is_published_for_a_brick_several_nodes_offer()
    {
        await SignInAsync();
        _fakes.Answers["main"] = new Recognition("n1/orders", null, null, ["n1", "n2"]);

        var (status, _) = await CallAsync(HttpMethod.Post, "/api/publish",
            new { path = "orders.yaml", hash = await HashAsync(), id = "orders", message = "m", placement = new { main = "n2" } });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("n2", LastSave().Placement!["main"]);
    }

    [Fact]
    public async Task A_node_that_does_not_offer_the_brick_is_refused_and_nothing_is_published()
    {
        await SignInAsync();
        _fakes.Answers["main"] = new Recognition("n1/orders", null, null, ["n1", "n2"]);

        var (status, _) = await CallAsync(HttpMethod.Post, "/api/publish",
            new { path = "orders.yaml", hash = await HashAsync(), id = "orders", message = "m", placement = new { main = "n3" } });

        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Null(_fakes.LastSaveBody);
    }
}
