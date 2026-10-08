using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
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
            (_, _, _, _) => Task.FromResult("{}"), new DesignService(IBrickLookup.None), [], [], new InMemorySecretsManager(), _link,
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
        Assert.Equal(JsonValueKind.Null, body.GetProperty("layout").ValueKind);
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
}
