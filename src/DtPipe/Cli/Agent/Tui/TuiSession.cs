using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Core.Security;
using Spectre.Console;
using Terminal.Gui.App;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>
/// The agent conversation, run inside one full-screen application: the mission turn, then every
/// follow-up, with the input line open in between. Turns, commands and the soft cancel all live
/// here; the surface below only lends the screen and the keyboard.
///
/// <para>
/// Nothing in this loop writes through Spectre. The verdict table, the pending question and the
/// failure guidance are printed by <see cref="RunAsync"/> once the application has handed the
/// terminal back — that is why a turn returns a <see cref="TurnSummaryModel"/> instead of printing
/// one, and why the post-mission menu is a set of typed commands rather than a selection prompt.
/// </para>
/// </summary>
internal sealed class TuiSession
{
    private readonly IAnsiConsole _console;
    private readonly AgentTui _tui;
    private readonly AgentExecutor _executor;
    private readonly string? _driverName;
    private readonly Func<string, string, Task<bool>>? _confirm;

    /// <param name="driverName">Forced toolkit driver; tests pass the managed one.</param>
    /// <param name="confirm">
    /// Overrides the real-write confirmation. Production leaves it null and gets the surface's
    /// modal dialog; a test supplies its own answer rather than driving a modal.
    /// </param>
    public TuiSession(IAnsiConsole console, AgentTui tui, AgentExecutor executor,
        string? driverName = null, Func<string, string, Task<bool>>? confirm = null)
    {
        _console = console;
        _tui = tui;
        _executor = executor;
        _driverName = driverName;
        _confirm = confirm;
    }

    /// <summary>
    /// Runs the whole conversation and returns the exit code of the last turn. Ctrl+C leaves
    /// through an <see cref="OperationCanceledException"/>, which the command reports as 130 (F16).
    /// </summary>
    public async Task<int> RunAsync(
        string mission,
        string model,
        string baseUrl,
        AgentOptions opts,
        int maxIterations,
        CancellationToken ct,
        Action<IApplication>? surfaceReady = null,
        Action<TuiScreen>? onScreen = null)
    {
        var log = new TranscriptLog();
        var view = _executor.CreateSurfaceView(log);
        var chrome = new TuiChrome(Title(model), Posture(opts), maxIterations);

        TurnSummaryModel? last = null;
        await new TuiApp(_console, _driverName).RunSessionAsync(
            chrome, log, view, _executor.Trajectory, _executor.LiveHeader,
            async surface => last = await LoopAsync(surface, view, mission, model, baseUrl, opts, maxIterations),
            ct, surfaceReady, onScreen);

        // The terminal is back. Now — and only now — the session's verdict reaches scrollback,
        // through the same sequencer the sequential path uses.
        if (last is null) return 0;

        _tui.ReportTurn(last, _executor.Trajectory, _executor.Mode, maxIterations);
        return last.ExitCode;
    }

    /// <summary>
    /// The conversation: run a turn, show its verdict, wait for the next line. A line starting with
    /// <c>/</c> is a command and never reaches the model.
    /// </summary>
    private async Task<TurnSummaryModel?> LoopAsync(
        TuiSurface surface, TuiTurnView view, string mission,
        string model, string baseUrl, AgentOptions opts, int maxIterations)
    {
        TurnSummaryModel? last = null;
        string? prompt = mission;

        while (true)
        {
            if (prompt is null)
            {
                var command = SessionCommand.Parse(await surface.ReadLineAsync());
                switch (command.Kind)
                {
                    case SessionCommandKind.Quit:
                        return last;

                    case SessionCommandKind.Prompt:
                        prompt = command.Text;
                        break;

                    case SessionCommandKind.Mode:
                        // F1 stays a per-turn invariant: the next turn rebuilds the role prompt and
                        // the tool allow-list from the new mode. No write gate moves — --apply and
                        // the --allow-* flags are launch-time only.
                        var mode = _executor.CycleMode();
                        Rechrome(surface, model, opts);
                        Note(surface, $"Mode is now {mode.ToString().ToLowerInvariant()}; it applies from the next turn.");
                        continue;

                    case SessionCommandKind.Review:
                        // The steps list and the detail panel *are* the session review here.
                        surface.Post(() => surface.Screen.FocusSteps());
                        Note(surface, "Steps ↑↓ · e/b errors · →/enter expand · ⇥ back to the input line.");
                        continue;

                    case SessionCommandKind.Save:
                        Save(surface, command.Text);
                        continue;

                    case SessionCommandKind.Execute:
                        await ExecuteAsync(surface, view, opts);
                        continue;

                    case SessionCommandKind.Help:
                        Note(surface, $"Type to ask the agent · {SessionCommand.Hint} · esc stops a turn · ^C leaves.");
                        continue;

                    case SessionCommandKind.Unknown:
                        Note(surface, $"/{command.Text} is not a command — {SessionCommand.Hint}.");
                        continue;

                    default:
                        continue;
                }
            }

            var softCancel = surface.BeginTurn();
            try
            {
                last = await _executor.RunTurnOnSurfaceAsync(prompt, model, baseUrl, opts, maxIterations,
                    view, softCancel, surface.SessionToken);
            }
            finally
            {
                surface.EndTurn();
            }

            // The agent's question belongs in the status band, right above the line that answers
            // it — the verdict projection already puts it there.
            Note(surface, last.VerdictLine());
            prompt = null;
        }
    }

    /// <summary>
    /// Runs the plan the planner produced, straight through the engine. Present → confirm iff
    /// <c>--apply</c> → execute is <see cref="PlanExecution"/>'s policy, shared with the scrollback
    /// path; this method only supplies the surface's own gestures. Without <c>--apply</c> the run
    /// is a sample with the writer neutralised.
    /// </summary>
    private async Task ExecuteAsync(TuiSurface surface, TuiTurnView view, AgentOptions opts)
    {
        if (string.IsNullOrWhiteSpace(_executor.Trajectory.LastGeneratedYaml))
        {
            Note(surface, "No validated plan yet — ask the agent for one first.");
            return;
        }

        var result = await PlanExecution.RunAsync(_executor, opts,
            // The plan panel carries the topology on screen for the whole session; there is
            // nothing to render again before the question.
            present: _ => Task.CompletedTask,
            confirm: _ => (_confirm ?? surface.ConfirmAsync)("Execute plan", ConfirmMessage(view)),
            surface.SessionToken);

        if (result is null)
        {
            Note(surface, "Cancelled — nothing was written.");
            return;
        }

        view.ToolResult("execute-yaml-job", result.Content, result.IsError);
        Note(surface, result.IsError ? "The run reported an error — see the transcript."
            : opts.Apply ? "Plan executed." : "Dry-run complete — nothing was written.");
    }

    /// <summary>
    /// The modal's message: the write targets by name, taken from the plan panel's own topology so
    /// the user approves a write knowing where it lands rather than a bare yes/no. Connection
    /// strings are sanitised — a secret must not surface in the dialog.
    /// </summary>
    private static string ConfirmMessage(TuiTurnView view)
    {
        var targets = view.PlanSnapshot().Topology?.Branches
            .Select(b => b.Output)
            .Where(o => !string.IsNullOrWhiteSpace(o))
            .Select(o => ConnectionStringSanitizer.Sanitize(o!))
            .ToList();

        return targets is { Count: > 0 }
            ? "This will write to:\n  " + string.Join("\n  ", targets) + "\n\nExecute the plan and perform the write?"
            : "Execute this plan and perform a real write?";
    }

    private void Save(TuiSurface surface, string argument)
    {
        var yaml = _executor.Trajectory.LastGeneratedYaml;
        if (string.IsNullOrWhiteSpace(yaml))
        {
            Note(surface, "No plan to save yet.");
            return;
        }

        var path = string.IsNullOrWhiteSpace(argument) ? PlanFile.DefaultPath : argument;
        var failure = PlanFile.TrySave(path, yaml);
        Note(surface, failure is null ? $"Saved the plan to {path}." : $"Could not save to {path}: {failure}");
    }

    private static void Note(TuiSurface surface, string line)
        => surface.Post(() => surface.Screen.ShowStatus(line));

    private void Rechrome(TuiSurface surface, string model, AgentOptions opts)
    {
        var (title, posture) = (Title(model), Posture(opts));
        surface.Post(() => surface.Screen.Rechrome(title, posture));
    }

    private string Title(string model)
        => $"dtpipe agent · {model} · {_executor.Mode.ToString().ToLowerInvariant()}";

    private string Posture(AgentOptions opts)
        => AgentTui.StatusText(_executor.Mode, opts.Detail, opts.Apply);
}
