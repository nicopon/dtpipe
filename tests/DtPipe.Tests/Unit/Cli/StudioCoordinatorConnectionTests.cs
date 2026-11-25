using System.Net;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The coordinator connected from the page, against a fake coordinator on the loopback: a key belongs to the origin it was kept
/// for and no answer of a route holds it, a command-line coordinator is replaced for the session without touching the file, and
/// the link is not closed under a publication that holds it.
/// </summary>
[Collection("console-serial")]
public sealed class StudioCoordinatorConnectionTests : IAsyncLifetime
{
    private const string Key = "dtp_alice_0123456789abcdefghijklmnopqrstuvwxyzABCDEFG";

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-coordinator-connection-").FullName;
    private readonly StudioFakes _fakes = new() { Key = Key, Auth = [PersonAuth.Idp, PersonAuth.Key] };
    private readonly InMemorySecretsManager _secrets = new();
    private readonly HttpClient _http = new(new HttpClientHandler { AllowAutoRedirect = false });
    private StudioServer? _studio;
    private StudioSettings? _settings;

    public async ValueTask InitializeAsync()
    {
        await _fakes.StartAsync();
        File.WriteAllText(Path.Combine(_dir, "orders.yaml"), "# Orders\nmain:\n  input: duck:${{DB}}\n  output: csv:o.csv\n");
    }

    public async ValueTask DisposeAsync()
    {
        if (_studio is not null) await _studio.DisposeAsync();
        await _fakes.DisposeAsync();
        _http.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private string SettingsPath => Path.Combine(_dir, "state", "studio.json");

    private async Task StartAsync(CoordinatorLink? flag = null)
    {
        _settings = new StudioSettings(SettingsPath, _secrets);
        _studio = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            new FakeStudioEngine(), new DesignService(IBrickLookup.None), [], [], _secrets, flag, lookup => new DesignService(lookup),
            CoordinatorStart: new StudioCoordinatorStart(_settings)), Ct);
    }

    private async Task<(HttpStatusCode Status, string Body)> CallAsync(HttpMethod method, string path, object? body = null)
    {
        using var request = new HttpRequestMessage(method, $"http://127.0.0.1:{_studio!.Port}{path}");
        request.Headers.Add(StudioServer.TokenHeader, _studio.Token);
        if (body is not null) request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, Ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(Ct));
    }

    private async Task SignedInAsync()
    {
        for (var i = 0; i < 100; i++)
        {
            var (_, status) = await CallAsync(HttpMethod.Get, "/api/coordinator");
            if (status.Contains("\"signed-in\"")) return;
            await Task.Delay(50, Ct);
        }
        Assert.Fail("The key did not sign in.");
    }

    [Fact]
    public async Task A_key_is_kept_for_its_origin_and_no_answer_of_a_route_holds_it()
    {
        await StartAsync();
        var connect = new { origin = _fakes.CoordinatorUrl, auth = "key", key = Key, remember = true };

        var test = await CallAsync(HttpMethod.Post, "/api/coordinator/connection/test", connect);
        var put = await CallAsync(HttpMethod.Put, "/api/coordinator/connection", connect);
        await SignedInAsync();
        var get = await CallAsync(HttpMethod.Get, "/api/coordinator/connection");
        var status = await CallAsync(HttpMethod.Get, "/api/coordinator");
        var bad = await CallAsync(HttpMethod.Put, "/api/coordinator/connection", new { origin = _fakes.CoordinatorUrl, auth = "key", key = Key + "x", remember = true });

        Assert.Equal(HttpStatusCode.OK, test.Status);
        Assert.Equal(HttpStatusCode.OK, put.Status);
        Assert.Equal("stored", JsonDocument.Parse(get.Body).RootElement.GetProperty("key").GetString());
        foreach (var answer in new[] { test.Body, put.Body, get.Body, status.Body, bad.Body })
            Assert.DoesNotContain(Key[^20..], answer);

        // What the next start reads: the file holds no secret, the alias follows the origin.
        Assert.DoesNotContain(Key[^20..], File.ReadAllText(SettingsPath));
        var next = new StudioSettings(SettingsPath, _secrets);
        Assert.Equal(new CoordinatorSettings(_fakes.CoordinatorUrl, "key"), next.ReadCoordinator());
        Assert.Equal(Key, next.ReadCoordinatorKey(_fakes.CoordinatorUrl));
        Assert.Null(next.ReadCoordinatorKey("https://elsewhere.example.com"));

        // Forgetting disconnects: the routes that need the link refuse again, the file and the key are gone.
        Assert.Equal(HttpStatusCode.OK, (await CallAsync(HttpMethod.Delete, "/api/coordinator/connection")).Status);
        Assert.Equal(HttpStatusCode.NotFound, (await CallAsync(HttpMethod.Post, "/api/publish/check", new { yaml = "a: 1" })).Status);
        Assert.Null(new StudioSettings(SettingsPath, _secrets).ReadCoordinator());
        Assert.Null(next.ReadCoordinatorKey(_fakes.CoordinatorUrl));
    }

    [Fact]
    public async Task Disconnecting_closes_the_link_for_the_session_and_keeps_the_file_and_the_key()
    {
        await StartAsync();
        await CallAsync(HttpMethod.Put, "/api/coordinator/connection", new { origin = _fakes.CoordinatorUrl, auth = "key", key = Key, remember = true });
        await SignedInAsync();

        var disconnect = await CallAsync(HttpMethod.Post, "/api/coordinator/connection/disconnect");

        Assert.Equal(HttpStatusCode.OK, disconnect.Status);
        Assert.DoesNotContain(Key[^20..], disconnect.Body);
        var view = JsonDocument.Parse(disconnect.Body).RootElement;
        Assert.False(view.GetProperty("connected").GetBoolean());
        Assert.Equal("stored", view.GetProperty("key").GetString());
        Assert.Equal(HttpStatusCode.NotFound, (await CallAsync(HttpMethod.Post, "/api/publish/check", new { yaml = "a: 1" })).Status);
        var next = new StudioSettings(SettingsPath, _secrets);
        Assert.Equal(new CoordinatorSettings(_fakes.CoordinatorUrl, "key"), next.ReadCoordinator());
        Assert.Equal(Key, next.ReadCoordinatorKey(_fakes.CoordinatorUrl));
    }

    [Fact]
    public async Task A_command_line_coordinator_is_replaced_for_the_session_and_the_file_is_not_written_unless_asked()
    {
        using var flag = new CoordinatorLink(new Uri(_fakes.CoordinatorUrl));
        await StartAsync(flag);

        var before = JsonDocument.Parse((await CallAsync(HttpMethod.Get, "/api/coordinator/connection")).Body).RootElement;
        Assert.Equal("flag", before.GetProperty("source").GetString());

        var other = _fakes.CoordinatorUrl.Replace("127.0.0.1", "localhost");
        Assert.Equal(HttpStatusCode.OK, (await CallAsync(HttpMethod.Put, "/api/coordinator/connection", new { origin = other, auth = "browser", remember = false })).Status);

        var after = JsonDocument.Parse((await CallAsync(HttpMethod.Get, "/api/coordinator/connection")).Body).RootElement;
        Assert.Equal(("page", other), (after.GetProperty("source").GetString(), after.GetProperty("origin").GetString()));
        Assert.Contains(other, (await CallAsync(HttpMethod.Get, "/api/coordinator")).Body);
        Assert.False(File.Exists(SettingsPath));

        await CallAsync(HttpMethod.Put, "/api/coordinator/connection", new { origin = other, auth = "browser", remember = true });
        Assert.Equal(new CoordinatorSettings(other, "browser"), new StudioSettings(SettingsPath, _secrets).ReadCoordinator());
    }

    [Fact]
    public async Task The_link_is_not_closed_under_a_publication_in_flight()
    {
        _fakes.SaveGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await StartAsync();
        var connect = new { origin = _fakes.CoordinatorUrl, auth = "key", key = Key, remember = false };
        await CallAsync(HttpMethod.Put, "/api/coordinator/connection", connect);
        await SignedInAsync();
        var hash = JsonDocument.Parse((await CallAsync(HttpMethod.Get, "/api/job?path=orders.yaml")).Body).RootElement.GetProperty("hash").GetString();

        var publishing = CallAsync(HttpMethod.Post, "/api/publish", new { path = "orders.yaml", hash, id = "orders", message = "from the studio" });
        await _fakes.SaveReached.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.Equal(HttpStatusCode.Conflict, (await CallAsync(HttpMethod.Put, "/api/coordinator/connection", connect)).Status);
        Assert.Equal(HttpStatusCode.Conflict, (await CallAsync(HttpMethod.Delete, "/api/coordinator/connection")).Status);
        Assert.Equal(HttpStatusCode.Conflict, (await CallAsync(HttpMethod.Post, "/api/coordinator/connection/disconnect")).Status);

        _fakes.SaveGate.SetResult();
        Assert.Equal(HttpStatusCode.OK, (await publishing).Status);
        Assert.Equal(HttpStatusCode.OK, (await CallAsync(HttpMethod.Put, "/api/coordinator/connection", connect)).Status);
    }
}
