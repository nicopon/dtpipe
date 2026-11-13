using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using DtPipe.Tests.Helpers;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The studio's agent through the real server, a scripted model in place of the provider: what the model is offered, a write
/// that waits for the person's answer and does nothing without it, and the open job joined to the system message unless it
/// holds a credential the person keeps here.
/// </summary>
[Collection("console-serial")]
public sealed class StudioAgentTests : IAsyncLifetime
{
    private const string Job = "main:\n  input: \"generate:3\"\n  output: \"csv:out.csv\"\n";

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-agent-").FullName;
    private readonly string _cwd = Directory.GetCurrentDirectory();
    private readonly HttpClient _http = new();
    private StudioServer? _server;

    public ValueTask InitializeAsync()
    {
        // A job's relative paths resolve from the folder, as dtpipe studio sets it.
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

    /// <summary>Answers each model call with the next scripted step, then with a closing text; keeps what each call was given.</summary>
    private sealed class ScriptedLlm(params Func<List<ChatMessage>, LlmResponse>[] steps) : ILlmClient
    {
        private readonly Queue<Func<List<ChatMessage>, LlmResponse>> _steps = new(steps);
        public List<List<ChatMessage>> Calls { get; } = [];
        public List<string> Tools { get; } = [];

        public string ProviderName => "scripted";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string> { "m" });

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            lock (Calls)
            {
                Calls.Add([.. messages]);
                Tools.AddRange(tools.Select(t => t.Name));
            }
            var step = _steps.Count > 0 ? _steps.Dequeue() : _ => new LlmResponse(new ChatMessage("assistant", "done"), true, null);
            return Task.FromResult(step(messages));
        }

        public string LastToolAnswer() { lock (Calls) return Calls[^1].Last(m => m.Role == "tool").Content ?? ""; }
    }

    private static Func<List<ChatMessage>, LlmResponse> Calls(string tool, object args) => _ =>
        new LlmResponse(new ChatMessage("assistant", "", null,
            [new ToolCall("c1", tool, JsonSerializer.SerializeToElement(args))]), true, null);

    private async Task StartAsync(ScriptedLlm? llm)
    {
        var services = new ServiceCollection();
        Program.ConfigureServices(services);
        _server = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            StudioEngines.Real(services.BuildServiceProvider()), new DesignService(IBrickLookup.None), [], [], new InMemorySecretsManager(),
            Agent: llm is null ? null : new StudioAgentSetup(llm, "http://127.0.0.1:1", "m", new McpToolProvider(McpCatalogue.Tools()))), Ct);
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

    private async Task<JsonElement> UntilAsync(Func<JsonElement, bool> done)
    {
        for (var i = 0; i < 300; i++)
        {
            var (_, status) = await CallAsync(HttpMethod.Get, "/api/agent");
            if (done(status)) return status;
            await Task.Delay(50, Ct);
        }
        throw new TimeoutException("the agent did not reach the state awaited");
    }

    private static bool Pending(JsonElement s) => s.GetProperty("pending").ValueKind == JsonValueKind.Object;
    private static bool Idle(JsonElement s) => !s.GetProperty("busy").GetBoolean();

    private async Task TurnAsync(string prompt, object? job = null) =>
        Assert.Equal(HttpStatusCode.Accepted, (await CallAsync(HttpMethod.Post, "/api/agent/turn", new { prompt, job })).Status);

    private async Task<HttpStatusCode> AnswerAsync(JsonElement card, string choice, string? digest = null) =>
        (await CallAsync(HttpMethod.Post, "/api/agent/answer", new { id = card.GetProperty("id").GetString(), choice, digest })).Status;

    [Fact]
    public async Task Without_llm_provider_the_agent_is_disabled_and_its_routes_refuse()
    {
        await StartAsync(null);

        var (status, body) = await CallAsync(HttpMethod.Get, "/api/agent");
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.False(body.GetProperty("enabled").GetBoolean());
        Assert.Equal(HttpStatusCode.NotFound, (await CallAsync(HttpMethod.Post, "/api/agent/turn", new { prompt = "x" })).Status);
    }

    [Fact]
    public async Task The_model_is_offered_the_studio_tools_and_never_one_that_writes_a_target()
    {
        var llm = new ScriptedLlm();
        await StartAsync(llm);

        await TurnAsync("hello");
        await UntilAsync(Idle);

        Assert.Contains("save-job", llm.Tools);
        Assert.DoesNotContain("run-job", llm.Tools);
        Assert.Contains("dry-run", llm.Tools);
        Assert.DoesNotContain("execute-yaml-job", llm.Tools);
    }

    [Theory]
    [InlineData("save")]
    [InlineData("refuse")]
    public async Task A_save_waits_for_the_person_and_writes_only_the_text_the_studio_composed(string choice)
    {
        var llm = new ScriptedLlm(Calls("save-job", new { path = "new.yaml", yamlContent = Job }));
        await StartAsync(llm);

        await TurnAsync("save it");
        var card = (await UntilAsync(Pending)).GetProperty("pending");
        Assert.Equal("save", card.GetProperty("kind").GetString());
        Assert.False(File.Exists(Path.Combine(_dir, "new.yaml")), "the file was written before the person answered");

        Assert.Equal(HttpStatusCode.NoContent, await AnswerAsync(card, choice));
        await UntilAsync(Idle);

        if (choice == "save")
        {
            Assert.Equal(card.GetProperty("body").GetProperty("yaml").GetString(), File.ReadAllText(Path.Combine(_dir, "new.yaml")));
            Assert.Contains("\"saved\"", llm.LastToolAnswer());
        }
        else
        {
            Assert.False(File.Exists(Path.Combine(_dir, "new.yaml")));
            Assert.Contains("\"refused\"", llm.LastToolAnswer());
        }
    }

    [Fact]
    public async Task A_job_the_validation_refuses_is_refused_before_the_person_is_shown_a_card()
    {
        const string unknownKey = "main:\n  source:\n    type: generate\n  output: \"csv:out.csv\"\n";
        var llm = new ScriptedLlm(Calls("save-job", new { path = "new.yaml", yamlContent = unknownKey }));
        await StartAsync(llm);

        await TurnAsync("save it");
        var status = await UntilAsync(Idle);

        Assert.False(Pending(status), "a card was shown for a job the validation refuses");
        Assert.False(File.Exists(Path.Combine(_dir, "new.yaml")));
        Assert.Contains("validate-yaml-job", llm.LastToolAnswer());
        Assert.Contains("source", llm.LastToolAnswer());
    }

    [Fact]
    public async Task A_file_changed_between_the_card_and_the_answer_is_not_overwritten()
    {
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), Job);
        var llm = new ScriptedLlm(Calls("save-job", new { path = "job.yaml", yamlContent = Job.Replace("generate:3", "generate:5") }));
        await StartAsync(llm);

        await TurnAsync("change it");
        var card = (await UntilAsync(Pending)).GetProperty("pending");
        File.WriteAllText(Path.Combine(_dir, "job.yaml"), "# edited by hand\n" + Job);

        Assert.Equal(HttpStatusCode.Conflict, await AnswerAsync(card, "save"));
        await UntilAsync(Idle);
        Assert.StartsWith("# edited by hand", File.ReadAllText(Path.Combine(_dir, "job.yaml")));
        Assert.Contains("nothing was written", llm.LastToolAnswer());
    }

    [Fact]
    public async Task The_open_job_reaches_the_system_message_unless_it_holds_a_credential_the_person_keeps_here()
    {
        var llm = new ScriptedLlm();
        await StartAsync(llm);
        async Task<JsonElement> CardsOf(string yaml) =>
            (await CallAsync(HttpMethod.Post, "/api/design/decompose", new { yaml })).Body.GetProperty("model");

        await TurnAsync("look", new { path = (string?)null, unsaved = true, model = await CardsOf(Job) });
        await UntilAsync(Idle);
        Assert.Contains("<open-job>\n", llm.Calls[^1][0].Content);
        Assert.Contains("generate:3", llm.Calls[^1][0].Content);

        var secret = Job.Replace("\"generate:3\"", "\"pg:Host=h;Username=u;Password=abc\"");
        var calls = llm.Calls.Count;
        await TurnAsync("look again", new { path = (string?)null, unsaved = true, model = await CardsOf(secret) });
        var card = (await UntilAsync(Pending)).GetProperty("pending");
        Assert.Equal("secrets", card.GetProperty("kind").GetString());
        Assert.Contains("main.input", card.GetProperty("body").GetProperty("at").EnumerateArray().Select(a => a.GetString()));
        Assert.DoesNotContain("abc", card.GetRawText());
        Assert.Equal(calls, llm.Calls.Count);

        Assert.Equal(HttpStatusCode.NoContent, await AnswerAsync(card, "cancel"));
        await UntilAsync(Idle);
        Assert.Equal(calls, llm.Calls.Count);

        await TurnAsync("look a third time", new { path = (string?)null, unsaved = true, model = await CardsOf(secret) });
        card = (await UntilAsync(Pending)).GetProperty("pending");
        Assert.DoesNotContain("omit", card.GetProperty("choices").EnumerateArray().Select(c => c.GetString()));
        Assert.Equal(HttpStatusCode.NoContent, await AnswerAsync(card, "send"));
        await UntilAsync(Idle);
        Assert.Contains("Password=abc", llm.Calls[^1][0].Content);
    }

    [Fact]
    public async Task A_turn_with_no_job_open_says_so_to_the_model()
    {
        var llm = new ScriptedLlm();
        await StartAsync(llm);
        await TurnAsync("hello");
        await UntilAsync(Idle);
        Assert.Contains(StudioAgent.NoOpenJob, llm.Calls[^1][0].Content);
        Assert.DoesNotContain("<open-job>\n", llm.Calls[^1][0].Content);
    }

    [Fact]
    public async Task A_job_a_call_carries_becomes_the_plan_the_page_takes_once_per_revision()
    {
        const string Plan = "main:\n  input: \"generate:100\"\n  output: \"csv:out.csv\"\n";
        var llm = new ScriptedLlm(
            Calls("validate-yaml-job", new { yamlContent = Plan }),
            Calls("dry-run", new { yamlContent = Plan }));
        await StartAsync(llm);
        await TurnAsync("make it 100");
        var status = await UntilAsync(s => Idle(s) && s.GetProperty("count").GetInt32() > 3);
        var job = status.GetProperty("job");
        Assert.Equal(1, job.GetProperty("revision").GetInt32());
        Assert.Equal(Plan, job.GetProperty("yaml").GetString());

        // A page that has taken this revision is not sent its text again.
        var (_, seen) = await CallAsync(HttpMethod.Get, "/api/agent?rev=1");
        Assert.Equal(1, seen.GetProperty("job").GetProperty("revision").GetInt32());
        Assert.Equal(JsonValueKind.Null, seen.GetProperty("job").GetProperty("yaml").ValueKind);
    }

    [Fact]
    public async Task A_turn_has_no_step_bound_and_the_meter_counts_turns_and_steps_and_reads_the_context()
    {
        // More calls than the old bound of a turn, then the model's closing text.
        var steps = Enumerable.Range(0, 30).Select(i => Calls("list-jobs", new { n = i })).ToArray();
        var llm = new ScriptedLlm(steps);
        await StartAsync(llm);
        await TurnAsync("go on");
        var status = await UntilAsync(s => Idle(s) && s.GetProperty("count").GetInt32() > 30);
        Assert.DoesNotContain("iteration limit", status.GetRawText());
        var meter = status.GetProperty("meter");
        Assert.Equal(1, meter.GetProperty("turn").GetInt32());
        Assert.True(meter.GetProperty("steps").GetInt32() >= 31);

        await TurnAsync("again");
        meter = (await UntilAsync(Idle)).GetProperty("meter");
        Assert.Equal(2, meter.GetProperty("turn").GetInt32());
    }
}
