using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Pipeline;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Agent;

public enum PostMissionAction
{
    ContinueDiscussion,
    ExecutePlan,
    SwitchMode,
    ViewDag,
    InspectTrajectory,
    SaveYaml,
    Exit
}

public class AgentTui
{
    private readonly IAnsiConsole _console;

    public AgentTui(IAnsiConsole console)
    {
        _console = console;
    }

    /// <summary>
    /// Header for a session: model, endpoint, operating mode, and one line on what the mode will
    /// and will not do — so a user who asked the agent to "create a file" in plan mode is told up
    /// front that a plan, not a file, is what comes back.
    /// </summary>
    public void RenderRunContext(string model, string url, AgentMode mode, AgentDetailLevel detail = AgentDetailLevel.Compact)
    {
        var rule = new Rule("[bold cyan]dtpipe AI Agent[/]")
        {
            Justification = Justify.Left
        };
        _console.Write(rule);
        _console.MarkupLine(
            $"[grey]Model:[/] [bold green]{Markup.Escape(model)}[/]  |  " +
            $"[grey]Endpoint:[/] [blue]{Markup.Escape(url)}[/]  |  " +
            $"[grey]Mode:[/] [bold]{Markup.Escape(mode.ToString().ToLowerInvariant())}[/]  |  " +
            $"[grey]Detail:[/] [bold]{Markup.Escape(detail.ToString().ToLowerInvariant())}[/]");
        _console.MarkupLine($"[grey]{Markup.Escape(DescribeMode(mode))}[/]");
        _console.WriteLine();
    }

    /// <summary>
    /// One dim line at a turn boundary: the operating mode, the write posture that mode carries,
    /// and the scrollback detail level. It states the run's posture — it never renders a verdict;
    /// that stays <see cref="RenderFinalSummary"/>'s alone.
    /// </summary>
    public void RenderStatusLine(AgentMode mode, AgentDetailLevel detail, bool apply)
        => _console.MarkupLine($"[grey]▸ {Markup.Escape(StatusText(mode, detail, apply))}[/]");

    /// <summary>The run's posture as plain text: mode, the write posture that mode carries, detail level.</summary>
    public static string StatusText(AgentMode mode, AgentDetailLevel detail, bool apply)
    {
        string posture = mode == AgentMode.Plan
            ? "plan"
            : $"{mode.ToString().ToLowerInvariant()} · {(apply ? "apply" : "dry-run")}";
        return $"{posture} · detail: {detail.ToString().ToLowerInvariant()}";
    }

    private static string DescribeMode(AgentMode mode) => mode switch
    {
        AgentMode.Plan =>
            "Plan mode: the agent designs and validates a pipeline, then stops — it does not run it and writes no files. "
            + "To execute: re-run with --mode execute, or save the generated YAML.",
        AgentMode.Execute =>
            "Execute mode: the agent may run pipelines. Real writes still need --apply plus approval; "
            + "destructive SQL and network access stay denied unless allowed.",
        AgentMode.Autonomous =>
            "Autonomous mode: the agent plans, then executes through the guardrails. Real writes still need --apply plus approval.",
        _ => string.Empty
    };

    public async Task<string?> SelectModelAsync(ILlmClient llmClient, string url)
    {
        var models = await llmClient.ListModelsAsync(url);
        if (models.Count == 0)
        {
            _console.MarkupLine($"[yellow]⚠️ Could not auto-discover {llmClient.ProviderName} models at endpoint.[/]");
            string defaultModel = llmClient.ProviderName == "ollama" ? "gemma4:12b-mlx" : "gpt-4o";
            var manualModel = _console.Prompt(
                new TextPrompt<string>($"Enter {llmClient.ProviderName} model name (e.g., [green]{defaultModel}[/]):")
                    .DefaultValue(defaultModel)
            );
            return manualModel;
        }

        var prompt = new SelectionPrompt<string>()
            .Title($"Select a [bold cyan]{llmClient.ProviderName} Model[/] for the mission:")
            .PageSize(10)
            .MoreChoicesText("[grey](Move up and down to reveal more models)[/]");

        foreach (var m in models.OrderBy(m => m))
        {
            prompt.AddChoice(m);
        }

        return _console.Prompt(prompt);
    }

    public string PromptUserMission()
    {
        return _console.Prompt(
            new TextPrompt<string>("Describe your [bold cyan]data integration task[/]:")
                .PromptStyle("yellow")
        );
    }

    public string PromptFollowUp()
    {
        _console.WriteLine();
        return _console.Prompt(
            new TextPrompt<string>("💬 [bold cyan]Follow-up prompt or question[/]:")
                .PromptStyle("yellow")
        );
    }

    /// <summary>A separator between the prompt exchange above and the step-by-step work below,
    /// followed by the status line for the turn about to run.</summary>
    public void RenderWorkingHeader(AgentMode mode, AgentDetailLevel detail, bool apply)
    {
        _console.WriteLine();
        _console.Write(new Rule("[dim]working[/]")
        {
            Justification = Justify.Left,
            Style = new Style(Color.Grey)
        });
        RenderStatusLine(mode, detail, apply);
    }

    /// <summary>
    /// Runs one model call inside a Spectre <c>Live</c> region: the model's reasoning and answer
    /// stream in place, bounded in height, then the region is cleared and the permanent digest for
    /// <paramref name="detail"/> is written to scrollback (<see cref="RenderStepDigest"/>).
    /// </summary>
    public async Task<LlmResponse> RunStreamingStepAsync(
        int step, int maxSteps, AgentDetailLevel detail,
        Func<ILlmStreamObserver, Task<LlmResponse>> call)
    {
        var view = new StreamingStepView(step, maxSteps);
        LlmResponse result = new(new ChatMessage("assistant", null), true, "the stream produced no response");
        var gate = new object();

        await _console.Live(view.Build())
            .AutoClear(true)
            .Overflow(VerticalOverflow.Ellipsis)
            .Cropping(VerticalOverflowCropping.Top)
            .StartAsync(async ctx =>
            {
                void Repaint() { lock (gate) { try { ctx.UpdateTarget(view.Build()); } catch { /* teardown race */ } } }
                using var ticker = new Timer(_ => Repaint(), null, 150, 150);
                result = await call(new LiveStreamObserver(view, Repaint));
                Repaint();
            });

        RenderStepDigest(step, maxSteps, view.Elapsed, result, detail, view.ToolName);
        return result;
    }

    /// <summary>
    /// Runs a whole turn inside one persistent <c>Live</c> region: the <see cref="AgentShell"/> frame
    /// (header · transcript tail · footer · input). <paramref name="header"/> is polled by the
    /// refresh ticker so the clock and token meter stay live between steps. The frame auto-clears on
    /// exit; the committed transcript is then replayed to real scrollback so the permanent record is
    /// clean lines, not a frozen frame — and <see cref="RenderFinalSummary"/> stays the sole verdict.
    /// </summary>
    internal async Task<T> RunInLiveShellAsync<T>(
        AgentShell shell,
        Func<(string clock, string meter)> header,
        Func<ITurnView, Task<T>> body)
    {
        T result = default!;
        int FrameHeight() => Math.Clamp(_console.Profile.Height - 1, 8, 46);

        try
        {
            await _console.Live(shell.RenderFrame(FrameHeight()))
                .AutoClear(true)
                .Overflow(VerticalOverflow.Crop)
                .Cropping(VerticalOverflowCropping.Top)
                .StartAsync(async ctx =>
                {
                    void Repaint()
                    {
                        var (clock, meter) = header();
                        shell.Clock = clock;
                        shell.Meter = meter;
                        try { ctx.UpdateTarget(shell.RenderFrame(FrameHeight())); } catch { /* teardown race */ }
                    }

                    using var ticker = new Timer(_ => Repaint(), null, 200, 200);
                    result = await body(new ShellTurnView(shell, Repaint));
                    Repaint();
                });
        }
        finally
        {
            foreach (var line in shell.Transcript)
                _console.MarkupLine(line);
            _console.WriteLine();
        }

        return result;
    }

    /// <summary>
    /// Writes the permanent scrollback for one finished step: the one-line trace, then — for an
    /// intermediate step — the model's stated intent, and the chain-of-thought preview or panel
    /// that <paramref name="detail"/> asks for. The single render path for both the streaming and
    /// the blocking loop, so a step reads the same either way.
    /// </summary>
    public void RenderStepDigest(int step, int maxSteps, TimeSpan elapsed, LlmResponse response,
        AgentDetailLevel detail, string? toolFallback = null)
    {
        foreach (var line in StepDigest.Lines(step, maxSteps, elapsed, response, toolFallback, detail))
            _console.MarkupLine(line);

        if (detail == AgentDetailLevel.Full && !string.IsNullOrWhiteSpace(response.Thinking))
        {
            _console.Write(new Panel(Markup.Escape(response.Thinking!.Trim()))
            {
                Header = new PanelHeader("[grey]chain of thought[/]"),
                Border = BoxBorder.Rounded,
                BorderStyle = new Style(Color.Grey35),
                Expand = true
            });
        }
    }

    public void RenderAgentResponse(string content)
    {
        if (string.IsNullOrWhiteSpace(content)) return;

        _console.WriteLine();
        var panel = new Panel(Markup.Escape(content))
        {
            Header = new PanelHeader("[bold green]🤖 dtpipe Agent[/]"),
            Border = BoxBorder.Rounded,
            Expand = true
        };
        _console.Write(panel);
    }

    /// <summary>
    /// Keyboard review of the session trajectory (voie 4 §6 suite, lot C). Uses the full-screen
    /// <see cref="SessionReview"/> on an interactive terminal; on a piped / non-ANSI console — or
    /// when stdin is redirected — it falls back to the inline <see cref="InspectTrajectory"/> list.
    /// </summary>
    public void ReviewSession(AgentTrajectory trajectory)
    {
        if (trajectory.Steps.Count == 0)
        {
            _console.MarkupLine("[yellow]No trajectory steps logged in this session yet.[/]");
            return;
        }

        bool interactive = _console.Profile.Capabilities.Interactive
            && _console.Profile.Capabilities.Ansi
            && !Console.IsInputRedirected;
        if (!interactive)
        {
            InspectTrajectory(trajectory);
            return;
        }

        var review = new SessionReview(trajectory.Steps);
        _console.Live(review.RenderFrame())
            .AutoClear(true)
            .Overflow(VerticalOverflow.Visible)
            .Start(ctx =>
            {
                while (true)
                {
                    var key = Console.ReadKey(intercept: true);
                    if (!review.Apply(key)) break;
                    ctx.UpdateTarget(review.RenderFrame());
                }
            });

        // Leave the step the reviewer stopped on in scrollback, so the review produces a record.
        if (review.Current is { } step)
            RenderStepDetails(step);
    }

    public void InspectTrajectory(AgentTrajectory trajectory)
    {
        if (trajectory.Steps.Count == 0)
        {
            _console.MarkupLine("[yellow]No trajectory steps logged in this session yet.[/]");
            return;
        }

        while (true)
        {
            _console.WriteLine();
            var prompt = new SelectionPrompt<string>()
                .Title("🧠 [bold cyan]Trajectory Step Inspector[/] (Select a step to view details):")
                .PageSize(12);

            var stepMap = new Dictionary<string, TrajectoryStep>();
            for (int i = 0; i < trajectory.Steps.Count; i++)
            {
                var s = trajectory.Steps[i];
                string statusIcon = s.IsError ? "⚠️" : (s.ToolName != null ? "🛠" : "💭");
                string label = $"{statusIcon} Step {s.Iteration} [[{s.Timestamp:HH:mm:ss}]]: {Markup.Escape(s.ToolName ?? "Reasoning")}";
                stepMap[label] = s;
                prompt.AddChoice(label);
            }

            const string backOption = "⬅ Back to main menu";
            prompt.AddChoice(backOption);

            var selected = _console.Prompt(prompt);
            if (selected == backOption)
                break;

            var step = stepMap[selected];
            RenderStepDetails(step);
        }
    }

    private void RenderStepDetails(TrajectoryStep step)
    {
        _console.WriteLine();
        var rule = new Rule($"[bold blue]Step {step.Iteration} Details ({step.Timestamp:HH:mm:ss})[/]")
        {
            Justification = Justify.Left
        };
        _console.Write(rule);

        if (step.Usage is { } u)
        {
            var parts = new List<string>();
            if (u.PromptTokens > 0) parts.Add($"prompt {u.PromptTokens} tok");
            if (u.CompletionTokens > 0) parts.Add($"output {u.CompletionTokens} tok");
            if (u.TokensPerSecond is { } tps) parts.Add($"{tps:F0} tok/s");
            if (u.GenerationTime is { } g) parts.Add($"gen {g.TotalSeconds:F1}s");
            if (parts.Count > 0)
                _console.MarkupLine($"[grey]{Markup.Escape(string.Join("  ·  ", parts))}[/]");
        }

        if (!string.IsNullOrWhiteSpace(step.Thinking))
        {
            _console.Write(new Panel(Markup.Escape(step.Thinking!))
            {
                Header = new PanelHeader("[grey]Chain of thought[/]"),
                Border = BoxBorder.Rounded,
                BorderStyle = new Style(Color.Grey35),
                Expand = true
            });
        }

        if (!string.IsNullOrWhiteSpace(step.Reasoning))
        {
            var reasoningPanel = new Panel(Markup.Escape(step.Reasoning))
            {
                Header = new PanelHeader("[yellow]Agent Reasoning / Intent[/]"),
                Border = BoxBorder.Rounded,
                Expand = true
            };
            _console.Write(reasoningPanel);
        }

        if (!string.IsNullOrEmpty(step.ToolName))
        {
            _console.MarkupLine($"[bold magenta]Tool Executed:[/] {Markup.Escape(step.ToolName)}");
            if (!string.IsNullOrWhiteSpace(step.ToolArgs))
            {
                var argsPanel = new Panel(Markup.Escape(step.ToolArgs))
                {
                    Header = new PanelHeader("[grey]Tool Arguments[/]"),
                    Border = BoxBorder.Square,
                    Expand = true
                };
                _console.Write(argsPanel);
            }

            if (!string.IsNullOrWhiteSpace(step.ToolResult))
            {
                string color = step.IsError ? "red" : "green";
                var resultPanel = new Panel(Markup.Escape(step.ToolResult))
                {
                    Header = new PanelHeader($"[bold {color}]Tool Output[/]"),
                    Border = BoxBorder.Rounded,
                    Expand = true
                };
                _console.Write(resultPanel);
            }
        }
    }

     /// <summary>
      /// Renders a single tool result (F5 parallel tool execution). Kept lightweight so that many
      /// independent calls produced in one turn can be rendered without heavy panels per call.
      /// </summary>
     public void RenderToolResult(string toolName, string result, bool isError)
        => _console.MarkupLine(StepDigest.ToolResultLine(toolName, result, isError));

     public void RenderPipelineDag(string yamlContent, IServiceProvider serviceProvider)
    {
        try
        {
            var secretsManager = serviceProvider.GetService<DtPipe.Cli.Security.ISecretsManager>();
            var jobs = JobFileParser.ParseContent(yamlContent, secretsManager);
            var streamTransformerFactories = serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
            var readerFactories = serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();

            var branches = jobs.Select(kv => new BranchDefinition
            {
                Alias = kv.Key,
                Input = kv.Value.Input,
                Output = kv.Value.Output,
                StreamingAliases = kv.Value.From != null
                    ? kv.Value.From.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    : Array.Empty<string>(),
                RefAliases = kv.Value.Ref ?? Array.Empty<string>(),
                Arguments = Array.Empty<string>(),
                ProcessorName = streamTransformerFactories
                    .FirstOrDefault(f => f.IsApplicable(kv.Value))
                    ?.ComponentName,
                PreParsedJob = kv.Value
            }).ToList();

            var dag = new JobDagDefinition { Branches = branches };

            _console.WriteLine();
            if (dag.Branches.Count > 1)
            {
                _console.Write(DagRenderer.BuildTopologyPanel(dag, readerFactories));
            }
            else if (dag.Branches.Count == 1)
            {
                _console.Write(DagRenderer.BuildLinearTopologyPanel(jobs.Values.First(), readerFactories));
            }
        }
        catch (Exception ex)
        {
            _console.MarkupLine($"[red]Could not render DAG topology:[/] {Markup.Escape(ex.Message)}");
        }
    }

    public void SaveYamlToFile(string yamlContent)
    {
        var filePath = _console.Prompt(
            new TextPrompt<string>("Enter file path to save the YAML pipeline:")
                .DefaultValue("pipeline.yaml")
        );

        try
        {
            File.WriteAllText(filePath, yamlContent);
            _console.MarkupLine($"[bold green]✓ Pipeline configuration saved to '[white]{Markup.Escape(filePath)}[/]'[/]");
        }
        catch (Exception ex)
        {
            _console.MarkupLine($"[bold red]❌ Failed to save file:[/] {Markup.Escape(ex.Message)}");
        }
    }

    public PostMissionAction ShowPostMissionMenu(bool hasYaml, bool apply, AgentMode mode)
    {
        _console.WriteLine();
        var prompt = new SelectionPrompt<string>()
            .Title("What would you like to do next?")
            .PageSize(10);

        const string continueOpt = "💬 Continue discussion / Refine pipeline";
        string executeOpt = apply
            ? "▶ Execute this plan now (real write)"
            : "▶ Execute this plan now (dry-run — nothing written)";
        string switchModeOpt = $"⇄ Switch mode (now: {mode.ToString().ToLowerInvariant()} → {AgentExecutor.NextMode(mode).ToString().ToLowerInvariant()})";
        const string viewDagOpt = "📊 View Pipeline DAG Topology";
        const string inspectOpt = "🧠 Inspect full step-by-step trajectory";
        const string saveYamlOpt = "💾 Save pipeline YAML file to disk";
        const string exitOpt = "🚪 Exit";

        prompt.AddChoice(continueOpt);
        if (hasYaml) prompt.AddChoice(executeOpt);
        prompt.AddChoice(switchModeOpt);
        if (hasYaml) prompt.AddChoice(viewDagOpt);
        prompt.AddChoice(inspectOpt);
        if (hasYaml) prompt.AddChoice(saveYamlOpt);
        prompt.AddChoice(exitOpt);

        var selected = _console.Prompt(prompt);

        return selected switch
        {
            continueOpt => PostMissionAction.ContinueDiscussion,
            _ when selected == executeOpt => PostMissionAction.ExecutePlan,
            _ when selected == switchModeOpt => PostMissionAction.SwitchMode,
            viewDagOpt => PostMissionAction.ViewDag,
            inspectOpt => PostMissionAction.InspectTrajectory,
            saveYamlOpt => PostMissionAction.SaveYaml,
            _ => PostMissionAction.Exit
        };
    }

    /// <summary>
    /// Announces a mid-session mode change. Switching into a mode that can execute states plainly
    /// what that opens and what it does not — <c>--apply</c> and the <c>--allow-*</c> flags are
    /// launch-time only and a switch never touches them.
    /// </summary>
    public void RenderModeSwitch(AgentMode newMode, bool apply)
    {
        _console.WriteLine();
        _console.MarkupLine($"[bold]Mode is now [cyan]{Markup.Escape(newMode.ToString().ToLowerInvariant())}[/].[/]");
        _console.MarkupLine($"[grey]{Markup.Escape(DescribeMode(newMode))}[/]");
        if (newMode != AgentMode.Plan)
        {
            string writeState = apply
                ? "writes are armed (--apply was set) and still ask for a confirmation."
                : "real writes are still off — the model can run pipelines but only as dry-runs until you relaunch with --apply.";
            _console.MarkupLine($"[grey]The model can now call execute-yaml-job; {Markup.Escape(writeState)} "
                + "Destructive SQL and network access stay denied unless allowed at launch.[/]");
        }
    }

    /// <summary>
    /// A last y/N gate before the validated plan performs a real write. This is stricter than the
    /// tool's own F2 path (a launch-time <c>--apply</c> already consented) — the analogue of
    /// confirming an exit from plan mode: you approve the plan you just read before it runs.
    /// </summary>
    public bool ConfirmRealWrite()
    {
        _console.WriteLine();
        return _console.Prompt(
            new ConfirmationPrompt("[yellow]Execute this plan and perform a real write?[/]")
            {
                DefaultValue = false
            });
    }

    /// <summary>Renders the outcome of running a validated plan through <c>execute-yaml-job</c>.</summary>
    public void RenderExecutionResult(string resultJson, bool isError)
    {
        _console.WriteLine();

        string? mode = null, nextStep = null, message = null, error = null;
        bool? applied = null, success = null;
        long? durationMs = null;
        var branchLines = new List<string>();

        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(resultJson);
            var root = doc.RootElement;
            if (root.TryGetProperty("mode", out var m)) mode = m.GetString();
            if (root.TryGetProperty("nextStep", out var ns) && ns.ValueKind == System.Text.Json.JsonValueKind.String) nextStep = ns.GetString();
            if (root.TryGetProperty("message", out var ms) && ms.ValueKind == System.Text.Json.JsonValueKind.String) message = ms.GetString();
            if (root.TryGetProperty("error", out var er) && er.ValueKind == System.Text.Json.JsonValueKind.String) error = er.GetString();
            if (root.TryGetProperty("applied", out var ap) && (ap.ValueKind == System.Text.Json.JsonValueKind.True || ap.ValueKind == System.Text.Json.JsonValueKind.False)) applied = ap.GetBoolean();
            if (root.TryGetProperty("success", out var sc) && (sc.ValueKind == System.Text.Json.JsonValueKind.True || sc.ValueKind == System.Text.Json.JsonValueKind.False)) success = sc.GetBoolean();
            if (root.TryGetProperty("durationMs", out var d) && d.ValueKind == System.Text.Json.JsonValueKind.Number) durationMs = d.GetInt64();
            if (root.TryGetProperty("errors", out var errs) && errs.ValueKind == System.Text.Json.JsonValueKind.Array)
                foreach (var e in errs.EnumerateArray())
                    if (e.ValueKind == System.Text.Json.JsonValueKind.String) error = (error is null ? "" : error + "\n") + e.GetString();
            if (root.TryGetProperty("branches", out var br) && br.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                foreach (var b in br.EnumerateArray())
                {
                    string alias = b.TryGetProperty("branch", out var ba) ? ba.GetString() ?? "?"
                        : b.TryGetProperty("Alias", out var al) ? al.GetString() ?? "?" : "?";
                    string counts = b.TryGetProperty("rowsRead", out var rr)
                        ? $"{rr} read → {(b.TryGetProperty("rowsDelivered", out var rd) ? rd.ToString() : "?")} delivered"
                        : "";
                    branchLines.Add($"  [grey]{Markup.Escape(alias)}[/] {Markup.Escape(counts)}");
                }
            }
        }
        catch
        {
            _console.Write(new Panel(Markup.Escape(resultJson))
            {
                Header = new PanelHeader("[grey]execution result[/]"),
                Border = BoxBorder.Rounded,
                Expand = true
            });
            return;
        }

        string head = (applied == true, isError || success == false) switch
        {
            (_, true) => "[bold red]❌ Execution failed[/]",
            (true, _) => "[bold green]🟢 Executed — data written[/]",
            _ => "[bold yellow]◻ Dry-run — nothing written[/]"
        };
        var rule = new Rule(head) { Justification = Justify.Left };
        _console.Write(rule);

        if (mode != null) _console.MarkupLine($"[grey]Mode:[/] {Markup.Escape(mode)}");
        if (durationMs is { } ms2) _console.MarkupLine($"[grey]Duration:[/] {ms2} ms");
        foreach (var line in branchLines) _console.MarkupLine(line);

        if (!string.IsNullOrWhiteSpace(error))
            _console.Write(new Panel(Markup.Escape(error!.Trim()))
            {
                Header = new PanelHeader("[bold red]Errors[/]"),
                Border = BoxBorder.Rounded,
                Expand = true
            });

        var note = nextStep ?? message;
        if (!string.IsNullOrWhiteSpace(note))
            _console.MarkupLine($"[grey]{Markup.Escape(note!.Trim())}[/]");
    }

    public void RenderFinalSummary(bool success, int iterations, Dictionary<string, int> toolCounts, TimeSpan duration, TurnOutcome outcome)
    {
        _console.WriteLine();
        var rule = new Rule("[bold cyan]Session Status[/]")
        {
            Justification = Justify.Left
        };
        _console.Write(rule);

        var table = new Table().Expand();
        table.AddColumn("[bold]Metric[/]");
        table.AddColumn("[bold]Value[/]");

        string statusMarkup = success ? "[bold green]🟢 COMPLETED[/]" : "[bold red]❌ INCOMPLETE[/]";
        table.AddRow("Status", statusMarkup);
        table.AddRow("Reason", Markup.Escape(DescribeOutcome(outcome)));
        table.AddRow("Iterations", iterations.ToString());
        table.AddRow("Duration", $"{duration.TotalSeconds:F2} seconds");

        if (toolCounts.Count > 0)
        {
            var toolSummary = string.Join(", ", toolCounts.Select(kv => $"{kv.Key}: {kv.Value}"));
            table.AddRow("Tool Calls", Markup.Escape(toolSummary));
        }

        _console.Write(table);
    }

    private static string DescribeOutcome(TurnOutcome outcome) => outcome switch
    {
        TurnOutcome.Succeeded => "delivered a response",
        TurnOutcome.MaxIterationsReached => "hit the iteration limit without finishing",
        TurnOutcome.LlmError => "the LLM call failed",
        TurnOutcome.EmptyResponse => "the model returned an empty response",
        TurnOutcome.RepetitionDetected => "the model got stuck repeating itself",
        _ => outcome.ToString()
    };

    /// <summary>
    /// Shown after the summary when a turn did not succeed: names the failure and, for the cases
    /// where partial work exists, prints the last recorded step so the user does not have to open
    /// the trajectory inspector to see where it stopped.
    /// </summary>
    public void RenderFailureGuidance(TurnOutcome outcome, AgentTrajectory trajectory, int maxIterations)
    {
        _console.WriteLine();
        switch (outcome)
        {
            case TurnOutcome.MaxIterationsReached:
                _console.MarkupLine($"[yellow]Reached the {maxIterations}-iteration limit without delivering a plan or an answer.[/]");
                _console.MarkupLine("[grey]Raise --max-iterations, narrow the request, or try a stronger model.[/]");
                RenderLastStep(trajectory);
                break;
            case TurnOutcome.EmptyResponse:
                _console.MarkupLine("[yellow]The model returned an empty response — nothing was produced.[/]");
                _console.MarkupLine("[grey]This model may not be tool-aware, or it emitted only hidden reasoning. Try another model.[/]");
                break;
            case TurnOutcome.LlmError:
                _console.MarkupLine("[grey]The LLM call failed (see the error above). Check the endpoint is reachable and the model name is correct.[/]");
                RenderLastStep(trajectory);
                break;
            case TurnOutcome.RepetitionDetected:
                _console.MarkupLine("[yellow]The model got stuck producing the same text and the call was stopped early.[/]");
                _console.MarkupLine("[grey]Retry with a temperature above 0 (this run used --temperature 0), or try a different model.[/]");
                break;
        }
    }

    private void RenderLastStep(AgentTrajectory trajectory)
    {
        var step = trajectory.Steps.LastOrDefault(s => s.IsError) ?? trajectory.Steps.LastOrDefault();
        if (step == null) return;

        var body = new System.Text.StringBuilder();
        if (!string.IsNullOrWhiteSpace(step.Reasoning))
            body.AppendLine(step.Reasoning.Trim());
        if (!string.IsNullOrEmpty(step.ToolName))
            body.AppendLine($"tool: {step.ToolName}");
        if (!string.IsNullOrWhiteSpace(step.ToolResult))
        {
            var r = step.ToolResult.Trim();
            body.Append(r.Length > 400 ? r[..400] + "…" : r);
        }

        _console.Write(new Panel(Markup.Escape(body.ToString().TrimEnd()))
        {
            Header = new PanelHeader("[grey]Last step before it stopped[/]"),
            Border = BoxBorder.Rounded,
            Expand = true
        });
        _console.MarkupLine("[grey]Full step-by-step detail: choose “Inspect full step-by-step trajectory” below.[/]");
    }

    /// <summary>Shown after a successful plan-mode turn: the plan exists but was not run.</summary>
    public void RenderPlanNextSteps()
    {
        _console.WriteLine();
        _console.MarkupLine(
            "[green]✓ Plan ready — not executed.[/] [grey]Run it with[/] [white]dtpipe agent --mode execute[/][grey], "
            + "or pick “Save pipeline YAML file to disk” below.[/]");
    }
}
