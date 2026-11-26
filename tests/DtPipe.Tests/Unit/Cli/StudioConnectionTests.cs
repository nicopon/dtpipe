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
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The agent's connection made from the page: what is remembered holds no secret, a key belongs to the address it was kept
/// for, no answer of the connection's routes carries the key, and a model is not swapped under a turn.
/// </summary>
[Collection("console-serial")]
public sealed class StudioConnectionTests : IAsyncLifetime
{
    private const string Key = "sk-test-0123456789abcdef";
    private const string Url = "https://API.Example.com:443/v1/";

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-connection-").FullName;
    private readonly string _cwd = Directory.GetCurrentDirectory();
    private readonly HttpClient _http = new();
    private readonly InMemorySecretsManager _secrets = new();
    private StudioServer? _server;
    private StudioSettings? _settings;

    public ValueTask InitializeAsync()
    {
        Directory.SetCurrentDirectory(_dir);
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

    private string SettingsPath => Path.Combine(_dir, "state", "studio.json");

    /// <summary>A client that echoes its key in its failures, as a server's error body may, and holds a turn while told to.</summary>
    private sealed class Echoing(string? key, TaskCompletionSource? hold) : ILlmClient
    {
        public string ProviderName => "echo";

        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) =>
            key is null ? Task.FromResult(new List<string> { "m" }) : throw new HttpRequestException($"401 Unauthorized, the key {key} is wrong");

        public async Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            if (hold is not null) await hold.Task.WaitAsync(ct);
            return new LlmResponse(new ChatMessage("assistant", "done"), true, null);
        }
    }

    private async Task StartAsync(TaskCompletionSource? hold = null)
    {
        _settings = new StudioSettings(SettingsPath, _secrets);
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        var tools = new McpToolProvider(McpCatalogue.Tools());
        _server = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            StudioEngines.Real(services.BuildServiceProvider()), new DesignService(IBrickLookup.None), [], [], _secrets,
            AgentLink: new StudioAgentLink(_settings, tools, null, (_, key, _) => new Echoing(key, hold))), Ct);
    }

    private async Task<(HttpStatusCode Status, string Body)> CallAsync(HttpMethod method, string path, object? body = null)
    {
        using var request = new HttpRequestMessage(method, $"http://127.0.0.1:{_server!.Port}{path}");
        request.Headers.Add(StudioServer.TokenHeader, _server.Token);
        if (body is not null) request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        using var response = await _http.SendAsync(request, Ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(Ct));
    }

    [Fact]
    public async Task A_key_is_kept_for_its_address_and_no_answer_of_a_route_holds_it()
    {
        await StartAsync();
        var connect = new { provider = "openai", url = Url, model = "m", timeout = 60, key = Key, remember = true };

        var test = await CallAsync(HttpMethod.Post, "/api/agent/connection/test", connect);
        var put = await CallAsync(HttpMethod.Put, "/api/agent/connection", connect);
        var get = await CallAsync(HttpMethod.Get, "/api/agent/connection");
        var disconnect = await CallAsync(HttpMethod.Post, "/api/agent/connection/disconnect");

        Assert.Equal(HttpStatusCode.BadGateway, test.Status);
        Assert.Equal(HttpStatusCode.OK, put.Status);
        Assert.Equal("stored", JsonDocument.Parse(put.Body).RootElement.GetProperty("key").GetString());
        foreach (var answer in new[] { test.Body, put.Body, get.Body, disconnect.Body })
            Assert.DoesNotContain(Key, answer);

        // Remembered once more, as the next start reads it: the file holds no secret, the alias follows the address.
        await CallAsync(HttpMethod.Put, "/api/agent/connection", connect);
        Assert.DoesNotContain(Key, File.ReadAllText(SettingsPath));
        var next = new StudioSettings(SettingsPath, _secrets);
        Assert.Equal(Key, next.ReadLlmKey("https://api.example.com/v1"));
        Assert.Null(next.ReadLlmKey("https://elsewhere.example.com/v1"));
    }

    [Fact]
    public async Task The_context_size_goes_through_the_connection_and_the_file_and_one_out_of_bounds_is_refused()
    {
        await StartAsync();
        var connect = new { provider = "ollama", url = "http://127.0.0.1:1", model = "m", numCtx = 8192, remember = true };

        var put = await CallAsync(HttpMethod.Put, "/api/agent/connection", connect);

        Assert.Equal(8192, JsonDocument.Parse(put.Body).RootElement.GetProperty("numCtx").GetInt32());
        Assert.Equal(8192, new StudioSettings(SettingsPath, _secrets).Read().Agent?.NumCtx);
        foreach (var numCtx in new[] { StudioAgentClients.MinNumCtx - 1, StudioAgentClients.MaxNumCtx + 1 })
            Assert.Equal(HttpStatusCode.BadRequest, (await CallAsync(HttpMethod.Put, "/api/agent/connection", new { provider = "ollama", url = "http://127.0.0.1:1", numCtx })).Status);

        var absent = await CallAsync(HttpMethod.Put, "/api/agent/connection", new { provider = "ollama", url = "http://127.0.0.1:1", model = "m", remember = true });
        Assert.Equal(AgentOptions.DefaultNumCtx, JsonDocument.Parse(absent.Body).RootElement.GetProperty("numCtx").GetInt32());
        Assert.Null(new StudioSettings(SettingsPath, _secrets).Read().Agent?.NumCtx);
    }

    [Fact]
    public async Task Disconnecting_closes_the_agent_and_keeps_the_file_and_the_key_and_a_turn_refuses_it()
    {
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await StartAsync(hold);
        var connect = new { provider = "openai", url = Url, model = "m", key = Key, remember = true };
        Assert.Equal(HttpStatusCode.OK, (await CallAsync(HttpMethod.Put, "/api/agent/connection", connect)).Status);
        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/agent/turn", new { prompt = "hello" })).Status);

        Assert.Equal(HttpStatusCode.Conflict, (await CallAsync(HttpMethod.Post, "/api/agent/connection/disconnect")).Status);
        Assert.True(JsonDocument.Parse((await CallAsync(HttpMethod.Get, "/api/agent/connection")).Body).RootElement.GetProperty("connected").GetBoolean());

        hold.SetResult();
        var disconnect = await CallAsync(HttpMethod.Post, "/api/agent/connection/disconnect");
        for (var i = 0; i < 100 && disconnect.Status == HttpStatusCode.Conflict; i++)
        {
            await Task.Delay(50, Ct);
            disconnect = await CallAsync(HttpMethod.Post, "/api/agent/connection/disconnect");
        }

        Assert.Equal(HttpStatusCode.OK, disconnect.Status);
        Assert.DoesNotContain(Key, disconnect.Body);
        Assert.False(JsonDocument.Parse(disconnect.Body).RootElement.GetProperty("connected").GetBoolean());
        Assert.Equal("m", new StudioSettings(SettingsPath, _secrets).Read().Agent?.Model);
        Assert.Equal(Key, new StudioSettings(SettingsPath, _secrets).ReadLlmKey(Url));
        Assert.Equal(HttpStatusCode.NotFound, (await CallAsync(HttpMethod.Post, "/api/agent/turn", new { prompt = "again" })).Status);
    }

    [Fact]
    public async Task A_model_is_not_swapped_under_a_turn()
    {
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await StartAsync(hold);
        var connect = new { provider = "ollama", url = "http://127.0.0.1:1", model = "m" };
        Assert.Equal(HttpStatusCode.OK, (await CallAsync(HttpMethod.Put, "/api/agent/connection", connect)).Status);
        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/agent/turn", new { prompt = "hello" })).Status);

        Assert.Equal(HttpStatusCode.Conflict, (await CallAsync(HttpMethod.Put, "/api/agent/connection", connect)).Status);

        hold.SetResult();
    }

    private static JsonElement Json(string body) => JsonDocument.Parse(body).RootElement.Clone();

    [Fact]
    public async Task The_recent_agents_grow_on_a_success_with_the_box_ticked_and_deleting_one_forgets_its_key_and_no_other()
    {
        await StartAsync();
        const string One = "https://one.example.com/v1";
        const string Two = "https://two.example.com/v1";
        var keyOne = Key + "-one";
        var keyTwo = Key + "-two";

        // Nothing is remembered with the box unticked, and a refused connection adds nothing.
        await CallAsync(HttpMethod.Put, "/api/agent/connection", new { provider = "openai", url = One, key = keyOne, remember = false });
        await CallAsync(HttpMethod.Put, "/api/agent/connection", new { provider = "nobody", url = One, remember = true });
        Assert.False(File.Exists(SettingsPath));

        await CallAsync(HttpMethod.Put, "/api/agent/connection", new { provider = "openai", url = One, model = "m", key = keyOne, remember = true });
        var put = await CallAsync(HttpMethod.Put, "/api/agent/connection", new { provider = "openai", url = Two, model = "n", key = keyTwo, remember = true });
        var get = await CallAsync(HttpMethod.Get, "/api/agent/connection");

        // The connection in place is the file's first entry and is not listed: the list shows the other values only.
        var history = Json(get.Body).GetProperty("history").EnumerateArray().ToList();
        Assert.Equal([Two, One], _settings!.ReadAgents().Select(a => a.Url));
        Assert.Equal([One], history.Select(h => h.GetProperty("url").GetString()));
        Assert.All(history, h => Assert.True(h.GetProperty("keyKept").GetBoolean()));
        Assert.Equal("m", history[0].GetProperty("model").GetString());

        // Deleting the entry of another address leaves the key of the connection in place.
        var other = await CallAsync(HttpMethod.Delete, "/api/agent/connection/history", new { provider = "openai", url = One });
        Assert.Equal(HttpStatusCode.OK, other.Status);
        Assert.Empty(Json(other.Body).GetProperty("history").EnumerateArray());
        Assert.Null(_settings.ReadLlmKey(One));
        Assert.Equal(keyTwo, _settings.ReadLlmKey(Two));

        // Disconnecting puts the value among the others; deleting it takes the entry, the remembered connection and the key of its address.
        var disconnect = await CallAsync(HttpMethod.Post, "/api/agent/connection/disconnect");
        var listed = Json(disconnect.Body).GetProperty("history").EnumerateArray().ToList();
        Assert.Equal([Two], listed.Select(h => h.GetProperty("url").GetString()));
        Assert.True(listed[0].GetProperty("keyKept").GetBoolean());
        var active = await CallAsync(HttpMethod.Delete, "/api/agent/connection/history", new { provider = "openai", url = Two });
        var view = Json(active.Body);
        Assert.Empty(view.GetProperty("history").EnumerateArray());
        Assert.False(view.GetProperty("connected").GetBoolean());
        Assert.Null(_settings.ReadLlmKey(Two));
        Assert.Null(_settings.Read().Agent);

        foreach (var answer in new[] { put.Body, get.Body, other.Body, disconnect.Body, active.Body })
        {
            Assert.DoesNotContain(keyOne, answer);
            Assert.DoesNotContain(keyTwo, answer);
        }
        Assert.DoesNotContain("sk-test", File.ReadAllText(SettingsPath));
    }
}
