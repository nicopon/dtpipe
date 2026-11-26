using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The folder opened from the page: a request after the change reads the new folder and the current directory follows it, the
/// agent starts a new conversation, and a change refused (unsaved changes, a run, a turn, a path that is no folder) changes nothing.
/// </summary>
[Collection("console-serial")]
public sealed class StudioWorkspaceTests : IAsyncLifetime
{
    private const string Linear = "main:\n  input: \"generate:2000000000\"\n  output: \"null:\"\n";

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-workspace-").FullName;
    private readonly string _cwd = Directory.GetCurrentDirectory();
    private readonly HttpClient _http = new();
    private readonly InMemorySecretsManager _secrets = new();
    private StudioServer? _server;
    private StudioSettings? _settings;

    private string First => Path.Combine(_dir, "first");
    private string Second => Path.Combine(_dir, "second");

    public ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(First);
        Directory.CreateDirectory(Second);
        File.WriteAllText(Path.Combine(First, "a.yaml"), Linear);
        File.WriteAllText(Path.Combine(Second, "b.yaml"), Linear);
        File.WriteAllText(Path.Combine(Second, "marker.txt"), "second");
        // As `dtpipe studio` does at start: the folder it opens is the current directory.
        Directory.SetCurrentDirectory(First);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_server is not null) await _server.DisposeAsync();
        Directory.SetCurrentDirectory(_cwd);
        _http.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A model that answers at once, or holds its turn until told to.</summary>
    private sealed class Model(TaskCompletionSource? hold) : ILlmClient
    {
        public string ProviderName => "ollama";

        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string> { "m" });

        public async Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            if (hold is not null) await hold.Task.WaitAsync(ct);
            return new LlmResponse(new ChatMessage("assistant", "done"), true, null);
        }
    }

    private async Task StartAsync(TaskCompletionSource? turn = null, TaskCompletionSource? run = null)
    {
        _settings = new StudioSettings(Path.Combine(_dir, "state", "studio.json"), _secrets);
        var engine = new FakeStudioEngine { Run = async (_, _, _, ct) => { if (run is not null) await run.Task.WaitAsync(ct); return 0; } };
        _server = await StudioServer.StartAsync(new StudioFolder(First), 0, new StudioTools(
            engine, new DesignService(IBrickLookup.None), [], [], _secrets,
            Agent: new StudioAgentSetup(new Model(turn), "http://127.0.0.1:1", "m", new McpToolProvider(McpCatalogue.Tools())),
            Settings: _settings), Ct);
    }

    private async Task<(HttpStatusCode Status, JsonElement Body)> CallAsync(HttpMethod method, string path, object? body = null)
    {
        using var request = new HttpRequestMessage(method, $"http://127.0.0.1:{_server!.Port}{path}");
        request.Headers.Add(StudioServer.TokenHeader, _server.Token);
        if (body is not null) request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, Ct);
        var text = await response.Content.ReadAsStringAsync(Ct);
        return (response.StatusCode, text.Length == 0 ? default : JsonDocument.Parse(text).RootElement.Clone());
    }

    private async Task<string[]> JobsAsync() =>
        [.. (await CallAsync(HttpMethod.Get, "/api/jobs")).Body.GetProperty("jobs").EnumerateArray().Select(j => j.GetString()!)];

    private async Task<JsonElement> UntilAsync(string path, Func<JsonElement, bool> done)
    {
        for (var i = 0; i < 300; i++)
        {
            var (_, body) = await CallAsync(HttpMethod.Get, path);
            if (done(body)) return body;
            await Task.Delay(50, Ct);
        }
        throw new TimeoutException(path);
    }

    private Task<(HttpStatusCode Status, JsonElement Body)> OpenAsync(string path, bool unsaved = false) =>
        CallAsync(HttpMethod.Put, "/api/workspace", new { path, remember = true, unsaved });

    [Fact]
    public async Task A_change_opens_the_new_folder_moves_the_current_directory_and_starts_a_new_conversation()
    {
        await StartAsync();
        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/agent/turn", new { prompt = "hello" })).Status);
        await UntilAsync("/api/agent", s => !s.GetProperty("busy").GetBoolean() && s.GetProperty("count").GetInt32() > 0);

        // A path that is no readable folder is refused with its reason, and nothing changes.
        Assert.Equal(HttpStatusCode.NotFound, (await OpenAsync(Path.Combine(Second, "missing"))).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await OpenAsync(Path.Combine(Second, "marker.txt"))).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await OpenAsync("second")).Status);
        Assert.Equal(["a.yaml"], await JobsAsync());

        var (tested, test) = await CallAsync(HttpMethod.Post, "/api/workspace/test", new { path = Second });
        Assert.Equal(HttpStatusCode.OK, tested);
        Assert.Equal(1, test.GetProperty("jobs").GetInt32());
        Assert.Equal(["a.yaml"], await JobsAsync());

        var (opened, view) = await OpenAsync(Second);
        Assert.Equal(HttpStatusCode.OK, opened);
        Assert.Equal("page", view.GetProperty("source").GetString());
        Assert.True(view.GetProperty("currentRemembered").GetBoolean());
        // The folder in place is the file's first entry and the list leaves it out.
        Assert.Equal([Second], _settings!.ReadFolders());
        Assert.Empty(view.GetProperty("recents").EnumerateArray());
        Assert.Equal(["b.yaml"], await JobsAsync());
        // A relative path resolves in the new folder, as a job's query file or local input does.
        Assert.Equal("second", File.ReadAllText("marker.txt"));
        Assert.Equal(0, (await CallAsync(HttpMethod.Get, "/api/agent")).Body.GetProperty("count").GetInt32());
        // The folder the studio started on is never remembered for it.
        Assert.DoesNotContain(First, _settings.ReadFolders());
    }

    [Fact]
    public async Task A_change_is_refused_naming_unsaved_changes_a_run_or_a_turn_and_changes_nothing()
    {
        var turn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var run = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await StartAsync(turn, run);

        var (unsaved, refusal) = await OpenAsync(Second, unsaved: true);
        Assert.Equal(HttpStatusCode.Conflict, unsaved);
        Assert.Contains("not saved", refusal.GetProperty("error").GetString());

        var hash = (await CallAsync(HttpMethod.Get, "/api/job?path=a.yaml")).Body.GetProperty("hash").GetString();
        var digest = (await CallAsync(HttpMethod.Post, "/api/run/prepare", new { path = "a.yaml", hash })).Body.GetProperty("digest").GetString();
        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/run", new { path = "a.yaml", hash, digest })).Status);
        var (running, during) = await OpenAsync(Second);
        Assert.Equal(HttpStatusCode.Conflict, running);
        Assert.Contains("run", during.GetProperty("error").GetString());
        run.SetResult();
        await UntilAsync("/api/run", s => !s.GetProperty("running").GetBoolean());

        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/agent/turn", new { prompt = "hello" })).Status);
        var (turning, waits) = await OpenAsync(Second);
        Assert.Equal(HttpStatusCode.Conflict, turning);
        Assert.Contains("turn", waits.GetProperty("error").GetString());
        turn.SetResult();

        Assert.Equal(["a.yaml"], await JobsAsync());
        Assert.True(File.Exists("a.yaml"));
        Assert.Equal("argument", (await CallAsync(HttpMethod.Get, "/api/workspace")).Body.GetProperty("source").GetString());
    }

    [Fact]
    public async Task A_recent_folder_is_removed_by_its_path_even_when_it_is_gone_and_the_folder_in_place_is_untouched()
    {
        await StartAsync();
        Assert.Equal(HttpStatusCode.OK, (await OpenAsync(Second)).Status);
        var gone = Path.Combine(_dir, "gone");
        _settings!.RememberFolder(gone);

        var (status, view) = await CallAsync(HttpMethod.Delete, "/api/workspace/recents", new { path = gone });

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Empty(view.GetProperty("recents").EnumerateArray());
        Assert.True(view.GetProperty("currentRemembered").GetBoolean());
        Assert.Equal([Second], _settings.ReadFolders());

        var (currentStatus, currentView) = await CallAsync(HttpMethod.Delete, "/api/workspace/recents", new { path = Second });
        Assert.Equal(HttpStatusCode.OK, currentStatus);
        Assert.False(currentView.GetProperty("currentRemembered").GetBoolean());
        Assert.Empty(_settings.ReadFolders());
        Assert.Equal(HttpStatusCode.BadRequest, (await CallAsync(HttpMethod.Delete, "/api/workspace/recents", new { path = "second" })).Status);
        Assert.Equal(["b.yaml"], await JobsAsync());
    }

    [Fact]
    public void Folder_confine_handles_filesystem_root_and_expand_home_refuses_empty_profile()
    {
        var rootPath = Path.GetPathRoot(_dir)!;
        var rootFolder = new StudioFolder(rootPath);
        Assert.Equal(Path.GetFullPath(Path.Combine(rootFolder.Root, "job.yaml")), rootFolder.Confine("job.yaml"));

        Assert.Equal("/home/alice", StudioServer.ExpandHome("~", "/home/alice"));
        Assert.Equal("/home/alice", StudioServer.ExpandHome("~/", "/home/alice"));
        Assert.Equal(Path.Join("/home/alice", "jobs"), StudioServer.ExpandHome("~/jobs", "/home/alice"));
        Assert.Throws<StudioRefusal>(() => StudioServer.ExpandHome("~/jobs", ""));
    }

    [Fact]
    public async Task SlotHolds_does_not_leak_on_read_failure_drains_async_and_guards_concurrent_replacements()
    {
        var holds = new SlotHolds("busy", "changing");

        Assert.Throws<InvalidOperationException>(() => holds.Hold<int>(() => throw new InvalidOperationException("boom")));
        var replacedAfterError = false;
        await holds.ReplaceAsync(() => replacedAfterError = true, TimeSpan.FromSeconds(1), Ct);
        Assert.True(replacedAfterError);

        var lease = holds.Hold(() => 42);
        var draining = holds.ReplaceAsync(() => { }, TimeSpan.FromSeconds(5), Ct);

        // Wait until _draining is active before issuing concurrent requests/replacements.
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            var refused = Assert.Throws<StudioRefusal>(() => holds.Hold(() => 1));
            if (refused.Message == "changing") break;
            await Task.Delay(10, Ct);
        }

        var concurrentReplace = await Assert.ThrowsAsync<StudioRefusal>(() => holds.ReplaceAsync(() => { }, TimeSpan.FromSeconds(1), Ct));
        Assert.Equal(409, concurrentReplace.Status);

        lease.Dispose();
        await draining;

        // After the first replacement ends, the slot is usable again.
        using var after = holds.Hold(() => 99);
        Assert.Equal(99, after.Value);
    }
}
