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
          // F1: select the role prompt for the operating mode (PLAN forbids execution).
        Messages[0] = new ChatMessage("system", AgentSystemPrompt.Select(opts.Mode));
        Messages.Add(new ChatMessage("user", userPrompt));

        var toolCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var stopwatch = Stopwatch.StartNew();

        _tui.RenderWorkingHeader();

        // Primary run uses the instance Messages so the interactive / inspection flows keep state.
        var primary = await RunPlanningLoopAsync(Messages, userPrompt, model, baseUrl, opts, maxIterations,
            recordTrajectory: true, renderTui: true, ct);
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
                 new("system", AgentSystemPrompt.Select(opts.Mode)),
                 new("user", userPrompt)
               };
            var repl = await RunPlanningLoopAsync(fresh, userPrompt, model, baseUrl, opts, maxIterations,
                recordTrajectory: false, renderTui: false, ct);
            if (!string.IsNullOrWhiteSpace(repl.Yaml))
                observedYamls.Add(repl.Yaml);
         }

        Trajectory.Determinism = BuildDeterminismReport(repls, observedYamls);

        stopwatch.Stop();
        int shownIterations = turnIterations <= maxIterations ? turnIterations : maxIterations;
        _tui.RenderFinalSummary(success, shownIterations, toolCounts, stopwatch.Elapsed, primary.Outcome);

        if (!success)
            _tui.RenderFailureGuidance(primary.Outcome, Trajectory, maxIterations);
        else if (opts.Mode == AgentMode.Plan && !string.IsNullOrWhiteSpace(primary.Yaml))
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
        CancellationToken ct)
     {
        var toolCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
           // F1: the LLM only sees the tools allowed for the current mode (in PLAN mode,
          // 'execute-yaml-job' is filtered out so the model cannot drive execution).
        var availableTools = _toolProvider.GetToolDefinitions(opts.Mode);
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
            string? currentToolName = null;
            LlmUsage? currentUsage = null;

            var compactedMessages = _windowManager.Compact(messages, ContextStore);
            var iterationSw = Stopwatch.StartNew();

            LlmResponse response;
            if (useStreaming)
              {
                var streaming = (IStreamingLlmClient)_llmClient;
                response = await _tui.RunStreamingStepAsync(currentStepNum, maxIterations, opts.ShowThinking,
                    obs => streaming.ChatStreamAsync(baseUrl, model, compactedMessages, availableTools, obs,
                        opts.NumCtx, opts.Temperature, opts.Seed, ct));
              }
             else if (renderTui)
              {
                response = await _console.Status()
                      .Spinner(Spinner.Known.Dots)
                      .SpinnerStyle(Style.Parse("blue bold"))
                      .StartAsync<LlmResponse>($"Agent thinking (Step {currentStepNum})...",
                          _ => _llmClient.ChatAsync(baseUrl, model, compactedMessages, availableTools,
                              opts.NumCtx, temperature: opts.Temperature, seed: opts.Seed, ct));
              }
             else
              {
                response = await _llmClient.ChatAsync(baseUrl, model, compactedMessages, availableTools,
                    opts.NumCtx, temperature: opts.Temperature, seed: opts.Seed, ct);
              }

             iterationSw.Stop();

             if (!string.IsNullOrEmpty(response.Error))
             {
                string errMsg = response.Error!;
                if (renderTui)
                    _tui.RenderAgentResponse($"Error calling LLM: {errMsg}");
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
             if (response.Message.ToolCalls is { Count: > 0 })
                 currentToolName = response.Message.ToolCalls[0].Name;

             // The streaming view prints its own richer per-step line; only the blocking paths
             // need this one.
             if (renderTui && !useStreaming)
                 _tui.RenderCompactIterationStatus(currentStepNum, maxIterations, currentReasoning, currentToolName, iterationSw.Elapsed, currentUsage);

            var lastMsg = messages[^1];
            if (lastMsg.ToolCalls == null || lastMsg.ToolCalls.Count == 0)
             {
                if (!string.IsNullOrWhiteSpace(lastMsg.Content))
                 {
                     // The model finished the turn with a deliberate textual answer.
                    if (renderTui)
                        _tui.RenderAgentResponse(lastMsg.Content!);
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
                     _tui.RenderToolResult(call.Name, toolResultRaw, outcome.IsError);
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
