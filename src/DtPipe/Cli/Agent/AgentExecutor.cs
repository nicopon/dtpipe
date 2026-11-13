using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Mcp;

namespace DtPipe.Cli.Agent;

public class AgentExecutor
{
    private readonly IAgentToolProvider _toolProvider;
    private readonly ILlmClient _llmClient;
    private readonly ConversationWindowManager _windowManager = new();

    /// <summary>
    /// Fact store that survives conversation compaction (F4 — non-destructive context). Cached
    /// "fact" tool results (inspected schemas, sample rows, recent errors) are reloaded into the
    /// compacted window instead of being discarded. Exposed so callers/tests can inspect it.
    /// </summary>
    public AgentContextStore ContextStore { get; }

    public AgentTrajectory Trajectory { get; } = new();
    public List<ChatMessage> Messages { get; } = new();

    /// <summary>Why the most recent turn ended (null before the first turn).</summary>
    public TurnOutcome? LastTurnOutcome { get; private set; }

    /// <summary>
    /// The question the model asked with <c>ask-user</c>, when the last turn ended on
    /// <see cref="TurnOutcome.AwaitingUserInput"/>. Cleared at the start of the next turn — the
    /// user's answer becomes that turn's prompt.
    /// </summary>
    public AgentQuestion? PendingQuestion { get; private set; }

    /// <summary>
    /// The window's use told the person: the loop folded the older messages, or the window is nearly
    /// full and nothing more can be folded. Raised on the turn's thread, at most once of each kind per turn.
    /// </summary>
    public event Action<string>? ContextNotice;

    // The prompt and the answer of the last call, as the provider reported them: what the next prompt
    // starts from. Zero until a provider reports one, and always zero for a provider with no window to read.
    private int _lastWindowUse;
    // Once the window ran short the conversation stays folded: the use the provider reports is that of
    // the folded prompt, so reading it again would unfold the messages and send the next call over the line.
    private bool _foldedByUse;
    private bool _toldFolded, _toldFull;

    private long _turnTokens;
    private readonly Stopwatch _turnClock = new();
    private readonly AgentTrace? _trace;

    /// <summary>The session's diagnostic record, when one was asked for.</summary>
    internal AgentTrace? Trace => _trace;
    private bool _traceOpened;

    /// <summary>
    /// What the host joins to the role prompt for the next turns, such as the job the person has open: part of the system
    /// message, rewritten every turn and kept by compaction, so it stays unique and current. Null joins nothing.
    /// </summary>
    public string? Context { get; set; }

    /// <summary>
    /// The iteration bound of a host that sets none: a turn then ends by the model's answer, by the person's Stop, or by a guard
    /// of the loop (a decoding loop, a call that took too long, an output ceiling).
    /// </summary>
    internal const int Unbounded = int.MaxValue;

    public AgentExecutor(IAgentToolProvider toolProvider, ILlmClient llmClient, AgentContextStore? contextStore = null, AgentTrace? trace = null)
    {
        _toolProvider = toolProvider;
        _llmClient = llmClient;
        ContextStore = contextStore ?? new AgentContextStore();
        _trace = trace;
        if (_trace is not null) Trajectory.StepAdded += _trace.Step;

        Messages.Add(new ChatMessage("system", SystemPrompt));
    }

    /// <summary>
    /// Runs one agent turn and returns its verdict as data. <paramref name="view"/> is told what
    /// is in flight. <paramref name="stop"/> aborts the model call in flight only: tool calls run
    /// on <paramref name="ct"/> and are never interrupted mid-flight, and <paramref name="stop"/>
    /// must never be the process token, or a soft stop would report the 130 reserved for a real
    /// interrupt (F16).
    /// </summary>
    internal async Task<TurnSummaryModel> RunTurnAsync(
        string userPrompt,
        string model,
        string baseUrl,
        AgentOptions options,
        int maxIterations,
        ITurnView view,
        CancellationToken stop,
        CancellationToken ct)
    {
        PendingQuestion = null;   // this turn's prompt is the answer to any previous ask-user
        Messages[0] = new ChatMessage("system", SystemPrompt);
        Messages.Add(new ChatMessage("user", userPrompt));

        var availableTools = _toolProvider.GetToolDefinitions();

        if (_trace is not null)
        {
            // The launch context goes down once; the role prompt and the catalogue go down every
            // turn, because they are what that turn's model was given.
            if (!_traceOpened)
            {
                _traceOpened = true;
                _trace.Session(model, baseUrl, options, maxIterations);
            }
            _trace.SystemPrompt(SystemPrompt);
            _trace.Catalogue(availableTools);
            _trace.Turn(userPrompt);
        }

        _turnClock.Restart();
        _turnTokens = 0;
        _toldFolded = _toldFull = false;

        var primary = await RunPlanningLoopAsync(Messages, availableTools, model, baseUrl, options, maxIterations, view, stop, ct);
        LastTurnOutcome = primary.Outcome;
        PendingQuestion = primary.Outcome == TurnOutcome.AwaitingUserInput ? primary.Question : null;

        _turnClock.Stop();
        int shownIterations = primary.Iterations <= maxIterations ? primary.Iterations : maxIterations;
        var summary = new TurnSummaryModel(primary.Outcome, shownIterations, _turnClock.Elapsed, primary.ToolCounts, primary.Question,
            ProducedPlan: !string.IsNullOrWhiteSpace(primary.Yaml), Tokens: _turnTokens, ClosingWords: primary.ClosingWords);
        _trace?.Verdict(summary);
        return summary;
    }

    /// <summary>
    /// The messages of the next call. The count folds the conversation as it always did; the window's use
    /// folds it earlier, from <see cref="AgentOptions.CompactFrom"/> of what the provider reads, and
    /// <see cref="AgentOptions.WarnFrom"/> tells the person when folding is no longer enough. Only a
    /// provider that takes a window (<c>num_ctx</c>) has one to read.
    /// </summary>
    private List<ChatMessage> FitWindow(List<ChatMessage> messages, AgentOptions opts)
    {
        int window = _llmClient.ProviderName == "ollama" ? opts.NumCtx : 0;
        if (window > 0 && _lastWindowUse >= AgentOptions.CompactFrom * window) _foldedByUse = true;

        var fitted = _windowManager.Compact(messages, ContextStore, force: _foldedByUse);
        if (window <= 0 || _lastWindowUse < AgentOptions.CompactFrom * window) return fitted;

        string use = $"{_lastWindowUse:N0} of {window:N0} tokens";
        if (_lastWindowUse >= AgentOptions.WarnFrom * window)
        {
            if (!_toldFull)
            {
                _toldFull = true;
                Announce($"The model's context is nearly full ({use}): past it the provider drops the start of the conversation without saying so. Start a new conversation, or raise the window.");
            }
        }
        else if (!_toldFolded && fitted.Count < messages.Count)
        {
            _toldFolded = true;
            Announce($"The model's context is at {use}: the older messages are folded into the facts the agent keeps (schemas, samples, errors).");
        }
        return fitted;
    }

    private void Announce(string text)
    {
        _trace?.Context(text);
        ContextNotice?.Invoke(text);
    }

    /// <summary>Outcome of one run of the planning loop, including why it stopped.</summary>
    private sealed record PlanningLoopResult(
        bool Success,
        string? Yaml,
        int Iterations,
        Dictionary<string, int> ToolCounts,
        TurnOutcome Outcome,
        AgentQuestion? Question = null,
        string? ClosingWords = null);

    private async Task<PlanningLoopResult> RunPlanningLoopAsync(
        List<ChatMessage> messages,
        List<ToolDefinition> availableTools,
        string model,
        string baseUrl,
        AgentOptions opts,
        int maxIterations,
        ITurnView view,
        CancellationToken stop,
        CancellationToken ct)
    {
        var toolCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        // What each call already answered in this turn — see AnsweredCall.
        var answered = new Dictionary<string, AnsweredCall>(StringComparer.Ordinal);
        // F1: the LLM only sees the tools it is offered ('execute-yaml-job' is not among them, so
        // the model cannot drive execution), and a call naming any other tool is refused below,
        // whatever the provider behind would have done.
        var offeredNames = availableTools
            .Select(t => McpToolReflector.Normalise(t.Name))
            .ToHashSet(StringComparer.Ordinal);
        string? producedYaml = null;     // resolved plan, sourced from the yamlContent tool-call argument
        string? argYaml = null;          // from the yamlContent tool-call argument (the sole source, F6)

        // Stream token-by-token when the client can and the view can follow it; any other pairing
        // takes the blocking path (which still reports token stats).
        bool useStreaming = view.CanStream && _llmClient is IStreamingLlmClient;

        // A stop cancels the model call and nothing else: the token below is linked so a real
        // interrupt still cuts through, while tool calls keep running on `ct` alone. A call already
        // in flight against a database is not something a stop may abandon halfway.
        using var llmCancel = stop.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, stop)
            : null;
        var llmCt = llmCancel?.Token ?? ct;

        bool success = false;
        int turnIterations = 1;
        AgentQuestion? pendingQuestion = null;
        string? closingWords = null;
        // The value that stands if the loop exits by its own condition — the iteration budget ran
        // out before any explicit break set an outcome.
        var turnOutcome = TurnOutcome.MaxIterationsReached;

        while (turnIterations <= maxIterations)
        {
            int currentStepNum = Trajectory.Steps.Count + 1;
            string? currentReasoning = null;
            string? currentThinking = null;
            LlmUsage? currentUsage = null;

            var compactedMessages = FitWindow(messages, opts);

            LlmResponse response;
            try
            {
                if (useStreaming)
                {
                    var streaming = (IStreamingLlmClient)_llmClient;
                    response = await view.StreamingStepAsync(currentStepNum, maxIterations,
                        obs => streaming.ChatStreamAsync(baseUrl, model, compactedMessages, availableTools, obs,
                            opts.NumCtx, opts.Temperature, opts.Seed, llmCt));
                }
                else
                {
                    response = await view.BlockingStepAsync(currentStepNum,
                        c => _llmClient.ChatAsync(baseUrl, model, compactedMessages, availableTools,
                            opts.NumCtx, temperature: opts.Temperature, seed: opts.Seed, c), llmCt);
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested && !ct.IsCancellationRequested)
            {
                // The person stopped the model call. The session lives on, so this must not escape
                // as the cancellation a host turns into exit 130 — it is a stated outcome like any
                // other, and the steps taken so far stay in the trajectory.
                Trajectory.AddStep(currentStepNum, "Interrupted — you stopped the model call.");
                success = false;
                turnOutcome = TurnOutcome.UserInterrupted;
                break;
            }

            _turnTokens += response.Usage?.CompletionTokens ?? 0;
            if (response.Usage is { PromptTokens: > 0 } used) _lastWindowUse = used.PromptTokens + used.CompletionTokens;

            if (!string.IsNullOrEmpty(response.Error))
            {
                string errMsg = response.Error!;
                var failure = errMsg switch
                {
                    _ when string.Equals(errMsg, RepetitionGuard.DetectedMessage, StringComparison.Ordinal)
                        => TurnOutcome.RepetitionDetected,
                    _ when string.Equals(errMsg, TurnLimits.OutputCeilingMessage, StringComparison.Ordinal)
                        => TurnOutcome.OutputCeilingReached,
                    _ when TurnLimits.IsCallDeadline(errMsg) => TurnOutcome.CallTookTooLong,
                    _ => TurnOutcome.LlmError,
                };

                // Only an endpoint fault is announced as one. A ceiling, a deadline and a decoding
                // loop are ours, and labelling them "LLM Error" sends the reader to check a
                // connection that is working.
                var prefix = failure == TurnOutcome.LlmError ? "LLM Error: " : string.Empty;
                Trajectory.AddStep(currentStepNum, $"{prefix}{errMsg}",
                    thinking: response.Thinking ?? response.Message.Content, isError: true);
                success = false;
                turnOutcome = failure;
                break;
            }

            messages.Add(response.Message);
            currentReasoning = response.Message.Content;
            currentThinking = response.Thinking;
            currentUsage = response.Usage;

            var lastMsg = messages[^1];
            if (lastMsg.ToolCalls == null || lastMsg.ToolCalls.Count == 0)
            {
                if (!string.IsNullOrWhiteSpace(lastMsg.Content))
                {
                    // The model finished the turn with a deliberate textual answer — unless that
                    // answer IS the ask-user call, written out instead of emitted. A weak model
                    // does that, and the turn then reported success while the agent waited for a
                    // reply nobody had been asked for.
                    Trajectory.AddStep(currentStepNum, currentReasoning ?? "Finished response.", thinking: currentThinking, usage: currentUsage);

                    closingWords = lastMsg.Content;
                    pendingQuestion = AgentQuestion.FromTextualCall(lastMsg.Content);
                    success = pendingQuestion is null;
                    turnOutcome = pendingQuestion is null ? TurnOutcome.Succeeded : TurnOutcome.AwaitingUserInput;
                }
                else
                {
                    // No tool call and no text: the model stopped with nothing to show. Not a
                    // success: an empty turn must not read as "done".
                    Trajectory.AddStep(currentStepNum, "Model returned an empty response with no tool call.", isError: true, thinking: currentThinking, usage: currentUsage);
                    success = false;
                    turnOutcome = TurnOutcome.EmptyResponse;
                }
                break;
            }

            // Execute every tool call in this turn (F5). Calls that run no pipeline run in parallel.
            // Results are appended in the same order as the calls so each "tool" message stays
            // correlated with its call id.
            var allCalls = lastMsg.ToolCalls!;

            // 'ask-user' is a turn terminator, not a tool: the model needs a decision only the
            // user can make. It is never dispatched — the user's next message is the answer — but
            // any other calls in the same turn still run (F5). Extract it, run the rest, then stop.
            var askUserCall = allCalls.FirstOrDefault(c => McpToolProvider.IsAskUser(c.Name));
            var calls = askUserCall is null
                ? allCalls
                : allCalls.Where(c => !ReferenceEquals(c, askUserCall)).ToList();

            // F6: the yamlContent tool-call argument is the sole source of the plan YAML.
            // Collect it across all calls so the last one wins.
            string? yamlBeforeThisIteration = argYaml;
            foreach (var call in allCalls)
            {
                if (call.Arguments.ValueKind == JsonValueKind.Object &&
                    call.Arguments.TryGetProperty("yamlContent", out var yamlProp) &&
                    yamlProp.ValueKind == JsonValueKind.String)
                {
                    argYaml = yamlProp.GetString();
                }
            }

            // Surface a fresh plan to the turn view.
            if (!string.IsNullOrWhiteSpace(argYaml)
                && !string.Equals(argYaml, yamlBeforeThisIteration, StringComparison.Ordinal))
                view.PlanUpdated(argYaml!);

            // A call to a tool that was not offered is answered here, as an error, and never
            // reaches the provider. The answer takes the place of the result, so the call keeps its
            // "tool" message, its trajectory step and its line in the view.
            var outcomes = new ToolInvocationOutcome[calls.Count];
            var dispatched = new List<int>(calls.Count);
            for (int i = 0; i < calls.Count; i++)
            {
                if (offeredNames.Contains(McpToolReflector.Normalise(calls[i].Name)))
                    dispatched.Add(i);
                else
                    outcomes[i] = new ToolInvocationOutcome(
                        McpToolReflector.UnknownToolAnswer(calls[i].Name, availableTools.Select(t => t.Name)), true);
            }

            // A call that runs a pipeline waits on the process's one-run gate. Started together,
            // such calls would reach it in whatever order the scheduler gives: they run after the
            // others, one at a time, in the order the model emitted them.
            var runs = dispatched
                  .Where(i => _toolProvider.RunsPipeline(calls[i].Name))
                  .ToList();
            var others = dispatched.Except(runs).ToList();

            var tasks = others
                  .Select(i => InvokeToolInto(calls[i].Name, calls[i].Arguments, ct))
                  .ToArray();
            await Task.WhenAll(tasks);
            for (int k = 0; k < others.Count; k++)
                outcomes[others[k]] = await tasks[k];

            foreach (var i in runs)
                outcomes[i] = await InvokeToolInto(calls[i].Name, calls[i].Arguments, ct);

            foreach (var call in calls)
                toolCounts[call.Name] = toolCounts.GetValueOrDefault(call.Name, 0) + 1;

            for (int i = 0; i < calls.Count; i++)
            {
                var call = calls[i];
                var outcome = outcomes[i];
                string answer = outcome.Content ?? "{}";
                string argsFormatted = call.Arguments.ValueKind != JsonValueKind.Undefined ? call.Arguments.ToString() : "{}";

                // A repeat is established on the pair, not on the question: the call ran, and the
                // answer came back the same. Predicting instead — replaying a cached answer for a
                // question already asked — would have dtpipe assert "this cannot return anything
                // new" about a source another process is free to change, which is the one thing
                // this whole surface must never do.
                var callKey = AnsweredCall.KeyOf(call);
                string toolResultRaw = answer;
                if (answered.TryGetValue(callKey, out var prior) && prior.Answer == answer)
                    toolResultRaw = prior.Advise(answer);
                else
                    answered[callKey] = new AnsweredCall(Trajectory.Steps.Count + 1, answer);

                view.ToolResult(call.Name, toolResultRaw, outcome.IsError);
                Trajectory.AddStep(Trajectory.Steps.Count + 1, currentReasoning ?? "", call.Name, argsFormatted, toolResultRaw, outcome.IsError,
                    thinking: i == 0 ? currentThinking : null, usage: i == 0 ? currentUsage : null);

                messages.Add(new ChatMessage("tool", toolResultRaw, call.Name, ToolCallId: call.Id));

                // F4: cache "fact" tool results so they survive conversation compaction. The
                // fact key is derived from the call's arguments so re-inspections of the same
                // input overwrite rather than accumulate. The fact is the answer; the advice
                // wrapped around it is not one.
                RecordFactFor(call, answer, outcome.IsError);
            }

            producedYaml = argYaml;

            if (askUserCall is not null)
            {
                pendingQuestion = AgentQuestion.From(askUserCall.Arguments);
                const string ack = "{\"status\":\"question delivered to the user; awaiting their reply\"}";

                view.ToolResult("ask-user", ack, isError: false);
                Trajectory.AddStep(Trajectory.Steps.Count + 1, currentReasoning ?? "",
                    "ask-user", askUserCall.Arguments.ToString(), ack, isError: false);

                // The conversation stays valid for the next turn: every tool call gets a 'tool'
                // reply, and the user's answer follows as the next 'user' message.
                messages.Add(new ChatMessage("tool", ack, "ask-user", ToolCallId: askUserCall.Id));
                toolCounts["ask-user"] = toolCounts.GetValueOrDefault("ask-user", 0) + 1;

                success = false;
                turnOutcome = TurnOutcome.AwaitingUserInput;
                break;
            }

            turnIterations++;
        }

        // Promote the turn's YAML to the trajectory, where the plan a host offers is read from.
        if (!string.IsNullOrWhiteSpace(producedYaml))
            Trajectory.LastGeneratedYaml = producedYaml;

        return new PlanningLoopResult(success, producedYaml, turnIterations, toolCounts, turnOutcome, pendingQuestion, closingWords);
    }

    /// <summary>The system message in force. Read from two places — Messages[0] and the trace — so
    /// they cannot drift apart.</summary>
    private string SystemPrompt => Context is { Length: > 0 } context
        ? AgentSystemPrompt.Studio + "\n\n" + context
        : AgentSystemPrompt.Studio;

    private async Task<ToolInvocationOutcome> InvokeToolInto(string toolName, JsonElement args, CancellationToken ct)
    {
        try
        {
            var parsedResult = await _toolProvider.InvokeToolAsync(toolName, args, ct);
            return new ToolInvocationOutcome(parsedResult.Content, parsedResult.IsError);
        }
        catch (Exception ex)
        {
            return new ToolInvocationOutcome(JsonSerializer.Serialize(new { error = ex.Message }), true);
        }
    }

    /// <summary>
    /// Records a tool result as a surviving "fact" when the tool is one that produces reusable
    /// knowledge (F4). Only fact-producing tools are cached; their result key is derived from
    /// the call's arguments so re-inspecting the same target overwrites the previous fact.
    /// </summary>
    private void RecordFactFor(ToolCall call, string result, bool isError)
    {
        if (!IsFactProducingTool(call.Name))
            return;

        string key = BuildFactKey(call);
        ContextStore.RecordFact(key, call.Name, result, isError);
    }

    /// <summary>
    /// The tools whose results are reusable facts (schemas, samples, skeletons, errors). Named
    /// here rather than inline so a guard can hold them against the live catalogue: a renamed
    /// tool would otherwise stop being cached with nothing to show for it.
    /// </summary>
    internal static readonly IReadOnlySet<string> FactProducingTools =
         new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
              "inspect", "preview-data", "dry-run",
              "list-providers", "get-adapter-help", "get-transformer-help", "get-dag-topology",
            };

    private static bool IsFactProducingTool(string toolName) => FactProducingTools.Contains(toolName);

    /// <summary>
    /// Derives a stable fact key from a tool call's args. For input/query-bearing tools the
    /// key is "<tool> @ <input>[ #<query>]" so that, e.g., re-inspecting the same source
    /// overwrites the prior inspection rather than piling up duplicates.
    /// </summary>
    private static string BuildFactKey(ToolCall call)
    {
        if (call.Arguments.ValueKind == JsonValueKind.Object)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append(call.Name).Append(" @");

            string? input = FirstArgProperty(call.Arguments, "input", "source", "connection", "connectionString", "file");
            if (input != null)
                sb.Append(' ').Append(input);

            string? query = FirstArgProperty(call.Arguments, "query", "sql");
            if (query != null)
                sb.Append(" #").Append(query.Length > 40 ? query[..40] : query);

            if (sb.Length > 2)
                return sb.ToString().Trim();
        }

        return call.Name;
    }

    private static string? FirstArgProperty(JsonElement args, params string[] names)
    {
        foreach (var n in names)
        {
            if (args.TryGetProperty(n, out var v) && (v.ValueKind == JsonValueKind.String || v.ValueKind != JsonValueKind.Undefined))
            {
                if (v.ValueKind == JsonValueKind.String)
                    return v.GetString();
                return v.GetRawText();
            }
        }

        return null;
    }
}
