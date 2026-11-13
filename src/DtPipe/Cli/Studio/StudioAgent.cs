using System.Security.Cryptography;
using System.Text.Json;
using System.Text;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using DtPipe.Cli.Security;
using DtPipe.Core.Security;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The model the command line names for the studio's agent: its client, the address it calls (never set by the page), the model
/// to start with, dtpipe's tools as the model may be offered them, and the session trace when one was asked for.
/// </summary>
public sealed record StudioAgentSetup(ILlmClient Client, string BaseUrl, string? Model, IAgentToolProvider Tools, AgentTrace? Trace = null);

/// <summary>The job the person has open, as the server composed it from the page's cards.</summary>
internal sealed record OpenJob(string? Path, bool Unsaved, string Text);

/// <summary>
/// The agent of one studio: one conversation, in memory, its turn run in the background and read by polling. A write the model
/// asks for waits in its tool call for the person's answer to a card (<see cref="AskAsync"/>), with no delay: no answer, no
/// write. Stop ends the model call and any wait; a tool at work is never cut. Changing the model opens a new conversation.
/// </summary>
internal sealed class StudioAgent : IAsyncDisposable
{
    /// <summary>Above this, the open job is named but not joined: the model reads it with <c>read-job</c>.</summary>
    internal const int MaxJobChars = 16_000;
    private const int MaxText = 4_000;

    private readonly StudioAgentSetup _setup;
    private readonly IAgentToolProvider _tools;
    private readonly object _gate = new();
    private readonly CancellationTokenSource _shutdown = new();
    private readonly List<object> _log = [];
    // Texts whose literal secrets the person let reach the model, by hash, for the session.
    private readonly HashSet<string> _cleared = new(StringComparer.Ordinal);
    private AgentExecutor _executor;
    private LiveTurnView _view = new();
    private string? _model;
    private Task _turn = Task.CompletedTask;
    private CancellationTokenSource? _stop;
    private Card? _pending;
    private int _cards;
    // The last job a tool call carried and how many differing ones came: the page applies a revision it has not seen.
    private string? _planYaml;
    private int _planRevision;
    // What the panel's meter reads: the conversation's turns, the steps of the turn in flight, the prompt of the last call.
    private int _turns;
    private int _stepsInTurn;
    private int _contextTokens;
    private readonly AgentOptions _options = new();

    private sealed record Card(string Id, string Kind, object Body, string? Digest, string[] Choices, Func<string, string?, string?>? Check)
    {
        public TaskCompletionSource<string> Answer { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public StudioAgent(StudioAgentSetup setup, IAgentToolProvider tools)
    {
        _setup = setup;
        _tools = tools;
        _model = setup.Model;
        _executor = NewExecutor();
    }

    private AgentExecutor NewExecutor()
    {
        var executor = new AgentExecutor(_tools, _setup.Client, trace: _setup.Trace);
        executor.Trajectory.StepAdded += step => { lock (_gate) { _log.Add(StepEntry(step)); OfferPlan(step); Meter(step); } };
        executor.ContextNotice += text => Log(new { kind = "notice", text });
        return executor;
    }

    /// <summary>
    /// A job a tool call of the model carried (<c>yamlContent</c>) becomes the plan the page puts on the open job's cards
    /// as soon as the call returns, whatever the tool: a check, a dry-run, a preview. A save is not offered here: the page
    /// reads the saved file back, which is the text the person approved. A text the model repeats changes nothing.
    /// </summary>
    private void OfferPlan(TrajectoryStep step)
    {
        if (step.IsError || step.ToolArgs is null || McpToolReflector.Normalise(step.ToolName ?? "") == "savejob") return;
        string? yaml;
        try
        {
            using var args = JsonDocument.Parse(step.ToolArgs);
            yaml = args.RootElement.ValueKind == JsonValueKind.Object && args.RootElement.TryGetProperty("yamlContent", out var value) && value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : null;
        }
        catch (JsonException)
        {
            return;
        }
        if (string.IsNullOrWhiteSpace(yaml) || yaml == _planYaml) return;
        _planYaml = yaml;
        _planRevision++;
    }

    private void Meter(TrajectoryStep step)
    {
        _stepsInTurn++;
        if (step.Usage is { PromptTokens: > 0 } usage) _contextTokens = usage.PromptTokens;
    }

    /// <summary>The path a step saved, when its result says the person let the write through.</summary>
    private static string? SavedPath(TrajectoryStep step)
    {
        if (step.IsError || step.ToolResult is null || McpToolReflector.Normalise(step.ToolName ?? "") != "savejob") return null;
        try
        {
            using var result = JsonDocument.Parse(step.ToolResult);
            return result.RootElement.ValueKind == JsonValueKind.Object && result.RootElement.TryGetProperty("saved", out var saved) && saved.ValueKind == JsonValueKind.String
                ? saved.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    public bool Busy
    {
        get { lock (_gate) return !_turn.IsCompleted; }
    }

    /// <summary>The Stop of the turn in flight: a card's wait and the wait for the engine slot end with it.</summary>
    public CancellationToken TurnStop
    {
        get { lock (_gate) return _stop?.Token ?? CancellationToken.None; }
    }

    public void StartTurn(string prompt, string? model, OpenJob? job)
    {
        lock (_gate)
        {
            if (!_turn.IsCompleted) throw new StudioRefusal(409, "A turn is already running: stop it, or wait for it.");
            if (model is { Length: > 0 } && model != _model)
            {
                if (_log.Count > 0) ResetLocked();
                _model = model;
            }
            if (_model is null) throw new StudioRefusal(400, "Choose a model first.");
            _stop?.Dispose();
            _stop = new CancellationTokenSource();
            _log.Add(new { kind = "user", text = prompt });
            _turns++;
            _stepsInTurn = 0;
            var stop = _stop.Token;
            var chosen = _model;
            // Nothing of the request's call chain flows into the turn: no launch value, no console guard.
            using (ExecutionContext.SuppressFlow())
                _turn = Task.Run(() => TurnAsync(prompt, chosen, job, stop));
        }
    }

    private async Task TurnAsync(string prompt, string model, OpenJob? job, CancellationToken stop)
    {
        try
        {
            // The open job always goes with the message: the agent works on what the person sees.
            var context = NoOpenJob;
            if (job is not null)
            {
                var send = await MaySendAsync(job.Path, job.Text, ["send", "cancel"], stop);
                if (send != "send")
                {
                    Log(new { kind = "verdict", status = "cancelled", reason = "You cancelled the turn before the open job was sent." });
                    return;
                }
                context = OpenJobBlock(job);
            }
            _executor.Context = context;
            var summary = await _executor.RunTurnAsync(prompt, model, _setup.BaseUrl, _options, AgentExecutor.Unbounded, _view, stop, _shutdown.Token);
            Log(new
            {
                kind = "verdict",
                status = summary.Status.ToString().ToLowerInvariant(),
                reason = summary.Reason,
                closing = summary.ClosingWords,
                tools = summary.ToolSummary,
                duration = summary.DurationText,
            });
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested || _shutdown.IsCancellationRequested)
        {
            Log(new { kind = "verdict", status = "interrupted", reason = "The turn was stopped." });
        }
        catch (Exception ex)
        {
            Log(new { kind = "verdict", status = "failed", reason = ConnectionStringSanitizer.Sanitize(ex.Message) });
        }
    }

    /// <summary>
    /// Whether a job's text may reach the model: "send" when it holds no literal secret or the person let this very text
    /// go; otherwise the person answers a card that names where each secret is, never its value. The first choice sends.
    /// </summary>
    internal async Task<string> MaySendAsync(string? path, string text, string[] choices, CancellationToken ct)
    {
        var at = JobSecrets.Literals(text);
        if (at.Count == 0) return "send";
        var hash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
        lock (_gate)
            if (_cleared.Contains(hash)) return "send";
        var choice = await AskAsync("secrets", new { path, at }, null, choices, null, ct);
        if (choice == "send") lock (_gate) _cleared.Add(hash);
        return choice;
    }

    /// <summary>What the model reads when the page has no job open: a plan it gives opens as a new job.</summary>
    internal const string NoOpenJob = "The person has no job open in the designer: a job you give as 'yamlContent' opens there as a new job, not saved yet.";

    /// <summary>The open job as the model reads it: a block of data, or its name alone when it is too long to join.</summary>
    internal static string OpenJobBlock(OpenJob job)
    {
        var name = job.Path ?? "a new job, not saved yet";
        if (job.Text.Length > MaxJobChars)
            return $"The person has {name} open in the designer. At {job.Text.Length} characters it is too long to join here: read it with 'read-job' when you need it.";
        return $"The person has {name} open in the designer{(job.Unsaved ? ", with changes not saved yet" : "")}. Its text, as data:\n<open-job>\n{job.Text}\n</open-job>";
    }

    /// <summary>
    /// Shows a card and waits for the person's answer, as long as it takes. <paramref name="check"/> judges an answer when it
    /// arrives (the choice and the digest the page sends): a refusal ends the card as "changed" and answers the page 409.
    /// </summary>
    internal async Task<string> AskAsync(string kind, object body, string? digest, string[] choices, Func<string, string?, string?>? check, CancellationToken ct)
    {
        Card card;
        lock (_gate)
        {
            card = new Card($"c{++_cards}", kind, body, digest, choices, check);
            _pending = card;
        }
        try
        {
            using (ct.Register(() => card.Answer.TrySetCanceled(ct)))
                return await card.Answer.Task;
        }
        finally
        {
            lock (_gate)
                if (ReferenceEquals(_pending, card)) _pending = null;
        }
    }

    public void Answer(string? id, string? choice, string? digest)
    {
        Card card;
        lock (_gate)
            card = _pending is { } pending && pending.Id == id ? pending : throw new StudioRefusal(409, "No card waits under this id: it was answered, stopped or replaced.");
        if (choice is null || !card.Choices.Contains(choice))
            throw new StudioRefusal(400, $"Answer with one of: {string.Join(", ", card.Choices)}.");
        if (card.Check?.Invoke(choice, digest) is { } refusal)
        {
            card.Answer.TrySetResult("changed");
            throw new StudioRefusal(409, refusal);
        }
        card.Answer.TrySetResult(choice);
    }

    public void Stop()
    {
        lock (_gate)
        {
            if (_turn.IsCompleted) throw new StudioRefusal(409, "No turn is running.");
            _stop?.Cancel();
        }
    }

    public void Reset()
    {
        lock (_gate)
        {
            if (!_turn.IsCompleted) throw new StudioRefusal(409, "A turn is running: stop it before starting a new conversation.");
            ResetLocked();
        }
    }

    private void ResetLocked()
    {
        _executor = NewExecutor();
        _view = new LiveTurnView();
        _log.Clear();
        _turns = 0;
        _stepsInTurn = 0;
        _contextTokens = 0;
    }

    public void Note(string? text)
    {
        if (_setup.Trace is not { } trace) throw new StudioRefusal(409, "This studio keeps no agent trace: start it with --agent-trace.");
        if (string.IsNullOrWhiteSpace(text)) throw new StudioRefusal(400, "A note has a text.");
        trace.Note(text);
    }

    public async Task<object> ModelsAsync(CancellationToken ct)
    {
        try { return new { models = await _setup.Client.ListModelsAsync(_setup.BaseUrl, ct), model = _model }; }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or InvalidOperationException)
        {
            throw new StudioRefusal(502, "The model's server did not list its models: " + ConnectionStringSanitizer.Sanitize(ex.Message));
        }
    }

    /// <summary>The conversation from entry <paramref name="after"/> on, and what is in flight: all a page needs, as text.</summary>
    public object Status(int after, int seen = 0)
    {
        lock (_gate)
        {
            after = Math.Clamp(after, 0, _log.Count);
            var live = _view.LiveStepSnapshot();
            var plan = _view.PlanSnapshot();
            return new
            {
                enabled = true,
                provider = _setup.Client.ProviderName,
                endpoint = Origin(_setup.BaseUrl),
                local = IsLocal(_setup.BaseUrl),
                model = _model,
                trace = _setup.Trace is not null,
                busy = !_turn.IsCompleted,
                count = _log.Count,
                entries = _log.Skip(after).ToList(),
                live = live is { } step ? new { step = step.Iteration, tool = step.ToolName, text = Bound(ConnectionStringSanitizer.Sanitize(step.Text)) } : null,
                plan = new { state = plan.State.ToString(), message = plan.Message, yaml = _executor.Trajectory.LastGeneratedYaml },
                meter = new
                {
                    turn = _turns,
                    steps = _stepsInTurn,
                    // The prompt the provider read for the last call, as it reports it; the window only where it is asked for as num_ctx.
                    contextTokens = _contextTokens,
                    window = _setup.Client.ProviderName == "ollama" ? _options.NumCtx : (int?)null,
                },
                job = new { revision = _planRevision, yaml = _planRevision > seen ? _planYaml : null },
                question = _turn.IsCompleted && _executor.PendingQuestion is { } q ? new { text = q.Text, options = q.Options } : null,
                pending = _pending is { } card ? new { id = card.Id, kind = card.Kind, body = card.Body, digest = card.Digest, choices = card.Choices } : null,
            };
        }
    }

    private void Log(object entry)
    {
        lock (_gate) _log.Add(entry);
    }

    private static object StepEntry(TrajectoryStep step) => new
    {
        kind = "step",
        step = step.Iteration,
        text = Bound(step.Reasoning),
        tool = step.ToolName,
        saved = SavedPath(step),
        args = step.ToolArgs is null ? null : Bound(ConnectionStringSanitizer.Sanitize(step.ToolArgs)),
        result = step.ToolResult is null ? null : Bound(ConnectionStringSanitizer.Sanitize(step.ToolResult)),
        error = step.IsError,
    };

    private static string Bound(string? text) =>
        text is null ? "" : text.Length <= MaxText ? text : text[..MaxText] + $" … ({text.Length - MaxText} more characters)";

    private static string Origin(string url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) ? uri.GetLeftPart(UriPartial.Authority) : ConnectionStringSanitizer.Redact(url);

    /// <summary>Whether what the tools read stays on this machine: the model's address is the loopback.</summary>
    private static bool IsLocal(string url) => Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.IsLoopback;

    public async ValueTask DisposeAsync()
    {
        Task turn;
        lock (_gate)
        {
            _shutdown.Cancel();
            _stop?.Cancel();
            turn = _turn;
        }
        await Task.WhenAny(turn, Task.Delay(TimeSpan.FromSeconds(15)));
    }
}
