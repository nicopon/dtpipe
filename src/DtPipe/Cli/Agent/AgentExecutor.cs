using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Mcp;
using Spectre.Console;

namespace DtPipe.Cli.Agent;

public class AgentExecutor
{
    private readonly IAgentToolProvider _toolProvider;
    private readonly ILlmClient _llmClient;
    private readonly AgentTui _tui;
    private readonly IAnsiConsole _console;
    private readonly ConversationWindowManager _windowManager = new();

      /// <summary>
       /// Fact store that survives conversation compaction (F4 — non-destructive context). Cached
       /// "fact" tool results (inspected schemas, sample rows, recent errors) are reloaded into the
       /// compacted window instead of being discarded. Exposed so callers/tests can inspect it.
       /// </summary>
     public AgentContextStore ContextStore { get; }

     public AgentTrajectory Trajectory { get; } = new();
     public List<ChatMessage> Messages { get; } = new();

     /// <summary>Why the most recent turn ended (null before the first turn). Drives the exit code
     /// and the reason line in the session summary.</summary>
     public TurnOutcome? LastTurnOutcome { get; private set; }

     private AgentMode? _mode;
     private long _turnTokens;

     /// <summary>
     /// The session's live operating mode. Seeded from the first turn's <see cref="AgentOptions.Mode"/>
     /// and thereafter the authority for the role prompt and the tool allow-list — the model sees
     /// the tools for <em>this</em> value at the top of every turn (F1 is a per-turn invariant), so
     /// switching it between turns is safe by construction. It never loosens a write gate: <c>--apply</c>
     /// and the <c>--allow-*</c> flags stay launch-time only.
     /// </summary>
     public AgentMode Mode => _mode ?? AgentMode.Plan;

     /// <summary>The next mode in the cycle plan → execute → autonomous → plan.</summary>
     internal static AgentMode NextMode(AgentMode mode) => mode switch
     {
         AgentMode.Plan => AgentMode.Execute,
         AgentMode.Execute => AgentMode.Autonomous,
         _ => AgentMode.Plan,
     };

     /// <summary>Advances <see cref="Mode"/> one step around the cycle and returns the new value.</summary>
     public AgentMode CycleMode()
     {
         _mode = NextMode(Mode);
         return _mode.Value;
     }

     public AgentExecutor(IAgentToolProvider toolProvider, ILlmClient llmClient, AgentTui tui, IAnsiConsole console, AgentContextStore? contextStore = null)
        {
            _toolProvider = toolProvider;
            _llmClient = llmClient;
            _tui = tui;
            _console = console;
           ContextStore = contextStore ?? new AgentContextStore();

         Messages.Add(new ChatMessage("system", AgentSystemPrompt.DefaultSystemPrompt));
        }

    /// <summary>
    /// Runs a single agent turn. When <see cref="AgentOptions.Repeat"/> &gt; 1 the validated plan
    /// is replicated that many times (each from a fresh conversation) and a
    /// <see cref="DeterminismReport"/> is attached to the trajectory (F3 — determinism).
    /// </summary>
    public async Task<int> RunTurnAsync(
        string userPrompt,
        string model,
        string baseUrl,
        AgentOptions? options = null,
        int maxIterations = 25,
        CancellationToken ct = default)
     {
        var opts = options ?? new AgentOptions();
        _mode ??= opts.Mode;
          // F1: select the role prompt for the live operating mode (PLAN forbids execution).
        Messages[0] = new ChatMessage("system", AgentSystemPrompt.Select(Mode));
        Messages.Add(new ChatMessage("user", userPrompt));

        var toolCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var stopwatch = Stopwatch.StartNew();
        _turnTokens = 0;

        // The persistent shell (D3): one Live frame for the whole turn. Same gate as streaming —
        // an ANSI, interactive console with a stream-capable client and real stdin. Everything else
        // (piped, --no-stream, replication) keeps the sequential scrollback path unchanged.
        bool consoleCanLive = _console.Profile.Capabilities.Ansi && _console.Profile.Capabilities.Interactive;
        bool useShell = consoleCanLive && !opts.NoStream && _llmClient is IStreamingLlmClient
            && !Console.IsInputRedirected;

        // Primary run uses the instance Messages so the interactive / inspection flows keep state.
        PlanningLoopResult primary;
        if (useShell)
        {
            var shell = new AgentShell
            {
                Title = "dtpipe agent",
                Subtitle = $"{model} · {Mode.ToString().ToLowerInvariant()}",
                Status = ShellStatusLine(opts),
                Hints = "esc stop · ^O detail · ⇧⇥ mode",
            };
            primary = await _tui.RunInLiveShellAsync(
                shell,
                () => (FormatClock(stopwatch.Elapsed), _turnTokens > 0 ? $"{_turnTokens} tok" : string.Empty),
                view => RunPlanningLoopAsync(Messages, userPrompt, model, baseUrl, opts, maxIterations,
                    recordTrajectory: true, renderTui: true, view, ct));
        }
        else
        {
            _tui.RenderWorkingHeader(Mode, opts.Detail, opts.Apply);
            primary = await RunPlanningLoopAsync(Messages, userPrompt, model, baseUrl, opts, maxIterations,
                recordTrajectory: true, renderTui: true, new ScrollbackTurnView(_console, _tui), ct);
        }
        bool success = primary.Success;
        int turnIterations = primary.Iterations;
        LastTurnOutcome = primary.Outcome;
        foreach (var kv in primary.ToolCounts)
            toolCounts[kv.Key] = kv.Value;

        var observedYamls = new List<string>();
        if (!string.IsNullOrWhiteSpace(primary.Yaml))
            observedYamls.Add(primary.Yaml);

        // Replication (F3): re-run the planning loop from a fresh conversation with the same
        // temperature/seed and compare the generated YAML to measure variance.
        int repls = Math.Max(1, opts.Repeat);
        for (int r = 1; r < repls; r++)
         {
             var fresh = new List<ChatMessage>
               {
                 new("system", AgentSystemPrompt.Select(Mode)),
                 new("user", userPrompt)
               };
            var repl = await RunPlanningLoopAsync(fresh, userPrompt, model, baseUrl, opts, maxIterations,
                recordTrajectory: false, renderTui: false, view: null, ct);
            if (!string.IsNullOrWhiteSpace(repl.Yaml))
                observedYamls.Add(repl.Yaml);
         }

        Trajectory.Determinism = BuildDeterminismReport(repls, observedYamls);

        stopwatch.Stop();
        int shownIterations = turnIterations <= maxIterations ? turnIterations : maxIterations;
        _tui.RenderFinalSummary(success, shownIterations, toolCounts, stopwatch.Elapsed, primary.Outcome);

        if (!success)
            _tui.RenderFailureGuidance(primary.Outcome, Trajectory, maxIterations);
        else if (Mode == AgentMode.Plan && !string.IsNullOrWhiteSpace(primary.Yaml))
            _tui.RenderPlanNextSteps();

        return success ? 0 : 1;
     }

    /// <summary>Outcome of one run of the planning loop, including why it stopped.</summary>
    private sealed record PlanningLoopResult(
        bool Success,
        string? Yaml,
        int Iterations,
        Dictionary<string, int> ToolCounts,
        TurnOutcome Outcome);

    private async Task<PlanningLoopResult> RunPlanningLoopAsync(
        List<ChatMessage> messages,
        string userPrompt,
        string model,
        string baseUrl,
        AgentOptions opts,
        int maxIterations,
        bool recordTrajectory,
        bool renderTui,
        ITurnView? view,
        CancellationToken ct)
     {
        var toolCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
           // F1: the LLM only sees the tools allowed for the current mode (in PLAN mode,
          // 'execute-yaml-job' is filtered out so the model cannot drive execution).
        var availableTools = _toolProvider.GetToolDefinitions(Mode);
        string? producedYaml = null;     // resolved plan, sourced from the yamlContent tool-call argument
        string? argYaml = null;          // from the yamlContent tool-call argument (the sole source, F6)

        // Stream token-by-token into a live view when the client can, the caller renders a TUI, and
        // the console can host an in-place region. Replication runs (renderTui: false), --no-stream
        // and piped / dumb terminals take the blocking path (which still reports token stats).
        bool consoleCanLive = _console.Profile.Capabilities.Ansi && _console.Profile.Capabilities.Interactive;
        bool useStreaming = renderTui && !opts.NoStream && consoleCanLive && _llmClient is IStreamingLlmClient;

        bool success = false;
        int turnIterations = 1;
        // The value that stands if the loop exits by its own condition — the iteration budget ran
        // out before any explicit break set an outcome.
        var turnOutcome = TurnOutcome.MaxIterationsReached;

        while (turnIterations <= maxIterations)
         {
            int currentStepNum = Trajectory.Steps.Count + 1;
            string? currentReasoning = null;
            string? currentThinking = null;
            LlmUsage? currentUsage = null;

            var compactedMessages = _windowManager.Compact(messages, ContextStore);
            var iterationSw = Stopwatch.StartNew();

            LlmResponse response;
            if (useStreaming)
              {
                var streaming = (IStreamingLlmClient)_llmClient;
                response = await view!.StreamingStepAsync(currentStepNum, maxIterations, opts.Detail,
                    obs => streaming.ChatStreamAsync(baseUrl, model, compactedMessages, availableTools, obs,
                        opts.NumCtx, opts.Temperature, opts.Seed, ct));
              }
             else if (renderTui)
              {
                response = await view!.BlockingStepAsync(currentStepNum,
                    c => _llmClient.ChatAsync(baseUrl, model, compactedMessages, availableTools,
                        opts.NumCtx, temperature: opts.Temperature, seed: opts.Seed, c), ct);
              }
             else
              {
                response = await _llmClient.ChatAsync(baseUrl, model, compactedMessages, availableTools,
                    opts.NumCtx, temperature: opts.Temperature, seed: opts.Seed, ct);
              }

             iterationSw.Stop();
             _turnTokens += response.Usage?.CompletionTokens ?? 0;

             if (!string.IsNullOrEmpty(response.Error))
             {
                string errMsg = response.Error!;
                if (renderTui)
                    view!.AgentResponse($"Error calling LLM: {errMsg}");
                if (recordTrajectory)
                    Trajectory.AddStep(currentStepNum, $"LLM Error: {errMsg}", isError: true);
                success = false;
                turnOutcome = string.Equals(errMsg, RepetitionGuard.DetectedMessage, StringComparison.Ordinal)
                    ? TurnOutcome.RepetitionDetected
                    : TurnOutcome.LlmError;
                break;
             }

             messages.Add(response.Message);
             currentReasoning = response.Message.Content;
             currentThinking = response.Thinking;
             currentUsage = response.Usage;

             // The streaming step renders its own digest once the stream ends; the blocking step
             // does not, so the digest is written here.
             if (renderTui && !useStreaming)
                 view!.Digest(currentStepNum, maxIterations, iterationSw.Elapsed, response, opts.Detail);

            var lastMsg = messages[^1];
            if (lastMsg.ToolCalls == null || lastMsg.ToolCalls.Count == 0)
             {
                if (!string.IsNullOrWhiteSpace(lastMsg.Content))
                 {
                     // The model finished the turn with a deliberate textual answer.
                    if (renderTui)
                        view!.AgentResponse(lastMsg.Content!);
                    if (recordTrajectory)
                        Trajectory.AddStep(currentStepNum, currentReasoning ?? "Finished response.", thinking: currentThinking, usage: currentUsage);
                    success = true;
                    turnOutcome = TurnOutcome.Succeeded;
                 }
                else
                 {
                     // No tool call and no text: the model stopped with nothing to show. Not a
                     // success — this path used to report one, so an empty turn read as "done".
                    if (recordTrajectory)
                        Trajectory.AddStep(currentStepNum, "Model returned an empty response with no tool call.", isError: true, thinking: currentThinking, usage: currentUsage);
                    success = false;
                    turnOutcome = TurnOutcome.EmptyResponse;
                 }
                break;
             }

             // Execute every tool call in this turn (F5). Independent calls run in parallel by
             // default; --sequential forces one-at-a-time execution. Results are appended in the
             // same order as the calls so each "tool" message stays correlated with its call id.
             var calls = lastMsg.ToolCalls!;

                       // F6: the yamlContent tool-call argument is the sole source of the plan YAML.
                    // Collect it across all calls so the last one wins.
             foreach (var call in calls)
               {
                 if (call.Arguments.ValueKind == JsonValueKind.Object &&
                     call.Arguments.TryGetProperty("yamlContent", out var yamlProp) &&
                     yamlProp.ValueKind == JsonValueKind.String)
                   {
                      argYaml = yamlProp.GetString();
                   }
               }

             var outcomes = new List<ToolInvocationOutcome>(calls.Count);
             if (opts.Sequential)
               {
                 foreach (var call in calls)
                   {
                    outcomes.Add(await InvokeToolInto(call.Name, call.Arguments, ct));
                    toolCounts[call.Name] = toolCounts.GetValueOrDefault(call.Name, 0) + 1;
                   }
               }
             else
               {
                 var tasks = calls
                       .Select(c => InvokeToolInto(c.Name, c.Arguments, ct))
                       .ToArray();
                 await Task.WhenAll(tasks);
                 foreach (var (c, t) in calls.Zip(tasks))
                   {
                    var outcome = await t;
                    outcomes.Add(outcome);
                    toolCounts[c.Name] = toolCounts.GetValueOrDefault(c.Name, 0) + 1;
                   }
               }

             for (int i = 0; i < calls.Count; i++)
               {
                 var call = calls[i];
                 var outcome = outcomes[i];
                 string toolResultRaw = (outcome.Content ?? "{}");
                 string argsFormatted = call.Arguments.ValueKind != JsonValueKind.Undefined ? call.Arguments.ToString() : "{}";

                 toolResultRaw ??= "{}";
                 if (renderTui)
                     view!.ToolResult(call.Name, toolResultRaw, outcome.IsError);
                 if (recordTrajectory)
                     Trajectory.AddStep(Trajectory.Steps.Count + 1, currentReasoning ?? "", call.Name, argsFormatted, toolResultRaw, outcome.IsError,
                         thinking: i == 0 ? currentThinking : null, usage: i == 0 ? currentUsage : null);

                  messages.Add(new ChatMessage("tool", toolResultRaw, call.Name, ToolCallId: call.Id));

                   // F4: cache "fact" tool results so they survive conversation compaction. The
                   // fact key is derived from the call's arguments so re-inspections of the same
                   // input overwrite rather than accumulate.
                  RecordFactFor(call, toolResultRaw, outcome.IsError);
                 }

             producedYaml = argYaml;
             turnIterations++;
           }

        // Promote the primary run's YAML to the instance trajectory for interactive / inspection flows.
        if (recordTrajectory && !string.IsNullOrWhiteSpace(producedYaml))
            Trajectory.LastGeneratedYaml = producedYaml;

        return new PlanningLoopResult(success, producedYaml, turnIterations, toolCounts, turnOutcome);
     }

    /// <summary>
    /// Runs the validated plan the planner produced — the YAML on <see cref="AgentTrajectory.LastGeneratedYaml"/>
    /// — straight through the execution tool, never back through the LLM. This is the deterministic
    /// step <see cref="AgentMode.Plan"/>'s contract promises ("execution is a step run by the engine").
    /// The model never sees <c>execute-yaml-job</c> in Plan mode (F1); a human choosing to run the
    /// plan they just reviewed is the intended escape hatch, and the F2 guardrails inside the tool
    /// still apply — <c>apply=false</c> stays a sample run with the writer neutralised.
    /// </summary>
    public async Task<ToolResult> ExecuteValidatedPlanAsync(CancellationToken ct = default)
    {
        var yaml = Trajectory.LastGeneratedYaml;
        if (string.IsNullOrWhiteSpace(yaml))
            throw new InvalidOperationException("No validated plan to execute — the planner produced no YAML.");

        // F6: yamlContent is the sole plan source. Pass it through byte-for-byte.
        var args = JsonSerializer.SerializeToElement(new { yamlContent = yaml });
        return await _toolProvider.InvokeToolAsync("execute-yaml-job", args, ct);
    }

    private string ShellStatusLine(AgentOptions opts) => AgentTui.StatusText(Mode, opts.Detail, opts.Apply);

    private static string FormatClock(TimeSpan t) =>
        t.TotalMinutes >= 1
            ? $"{(int)t.TotalMinutes}m{t.Seconds:D2}s"
            : $"{t.TotalSeconds:F0}s";

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
    /// Builds the <see cref="DeterminismReport"/> from the YAMLs observed across replications.
    /// When only one replication was requested, a report is still produced (variance is 0 by
    /// definition) so the trajectory always carries a determinism signal.
    /// </summary>
    private static DeterminismReport BuildDeterminismReport(int repetitions, List<string> observedYamls)
     {
        var distinct = new List<string>();
        foreach (var y in observedYamls)
         {
            if (!distinct.Contains(y))
                distinct.Add(y);
         }

        return new DeterminismReport
          {
            Repetitions = repetitions,
            DistinctYaml = distinct
          };
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

       /// <summary>The tools whose results are reusable facts (schemas, samples, skeletons, errors).</summary>
     private static bool IsFactProducingTool(string toolName)
          => toolName is "inspect" or "preview-data" or "suggest-pipeline" or "dry-run" or "list-providers" or "get-adapter-help" or "get-transformer-help";

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
