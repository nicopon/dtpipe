using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Spectre.Console;
using Terminal.Gui.App;
using Terminal.Gui.Drivers;
using Terminal.Gui.Input;
using Terminal.Gui.Testing;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The full-screen layout — a steps list with its detail, a transcript
/// band, a focus-aware hint bar. Tab moves between the two focusable panels (the toolkit's own
/// navigation), Up/Down and e/b move the selection, Right expands the detail, and the detail
/// mirrors whichever step is highlighted. Driven headless: a state machine on a repeating timeout
/// runs each step on the UI thread, one per tick so an injected key is processed before the next.
/// </summary>
[Collection(TerminalGuiCollection.Name)]
public class TuiNavigationTests
{
    private static readonly TuiChrome Chrome = new("dtpipe agent · test", "plan · detail: compact");

    private static (string Clock, string Meter) Header() => ("3s", "42 tok");

    private static AgentTrajectory FourSteps()
    {
        var t = new AgentTrajectory();
        t.AddStep(1, "INTENT: discover the providers", toolName: "list-providers", toolResult: "[...]");
        t.AddStep(2, "INTENT: inspect the CSV", toolName: "inspect", toolResult: "boom", isError: true);
        t.AddStep(3, "INTENT: validate", toolName: "validate-yaml-job", toolResult: "ok");
        t.AddStep(4, "INTENT: dry-run the plan", toolName: "dry-run", toolResult: "5 rows");
        return t;
    }

    /// <summary>
    /// Opens the surface over <paramref name="trajectory"/> and runs <paramref name="steps"/> on the
    /// UI thread, one per timer tick. A step that throws stops the run and the exception surfaces.
    /// </summary>
    private static async Task Drive(AgentTrajectory trajectory, params Action<IApplication, TuiScreen>[] steps)
    {
        var sw = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(sw),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Profile.Width = 120;
        console.Profile.Height = 40;
        var app = new TuiApp(console, DriverRegistry.Names.DOTNET);

        var log = new TranscriptLog();
        var view = new TuiTurnView(log);
        var stop = new TaskCompletionSource();
        TuiScreen? screen = null;
        Exception? failure = null;

        await app.RunOneTurnAsync(Chrome, log, view, trajectory, Header,
            async (_, turnCt) =>
            {
                using var reg = turnCt.Register(() => stop.TrySetResult());
                await stop.Task;
                return 0;
            },
            CancellationToken.None,
            surfaceReady: live =>
            {
                int i = 0;
                int settleTicks = 3;   // let the first paint lay the panels out
                live.AddTimeout(TimeSpan.FromMilliseconds(40), () =>
                {
                    if (settleTicks-- > 0) return true;
                    try
                    {
                        if (i < steps.Length) { steps[i++](live, screen!); return true; }
                    }
                    catch (Exception ex)
                    {
                        failure = ex;
                    }
                    stop.TrySetResult();
                    return false;
                });
            },
            onScreen: s => screen = s);

        if (failure is not null) throw failure;
    }

    [Fact(Timeout = 30000)]
    public async Task The_Detail_Panel_Mirrors_The_Selected_Step()
    {
        await Drive(FourSteps(),
            (_, s) =>
            {
                // The panel tracks the tail while the turn runs — step 4 is selected.
                Assert.Equal(4, s.SelectedStepIteration);
                Assert.Contains("dry-run", s.DetailText);
                s.FocusSteps();
            },
            (app, _) => app.InjectKey(Key.CursorUp),
            (app, _) => app.InjectKey(Key.CursorUp),
            (_, s) =>
            {
                Assert.Equal(2, s.SelectedStepIteration);
                Assert.Contains("inspect", s.DetailText);
            });
    }

    [Fact(Timeout = 30000)]
    public async Task E_Jumps_To_The_Error_Step()
    {
        await Drive(FourSteps(),
            (_, s) => s.FocusSteps(),
            (app, _) => app.InjectKey(Key.Home),
            (app, _) => app.InjectKey(Key.E),
            (_, s) => Assert.Equal(2, s.SelectedStepIteration));   // the only error step
    }

    [Fact(Timeout = 30000)]
    public async Task Right_Expands_The_Detail_And_Left_Collapses_It()
    {
        var t = new AgentTrajectory();
        t.AddStep(1, "INTENT: go", toolName: "inspect", toolArgs: "{\"input\":\"csv:x\"}", toolResult: "ok",
            thinking: "a long private chain of thought");

        await Drive(t,
            (_, s) =>
            {
                s.FocusSteps();
                Assert.False(s.Expanded);
                Assert.DoesNotContain("chain of thought", s.DetailText);
            },
            (app, _) => app.InjectKey(Key.CursorRight),
            (_, s) =>
            {
                Assert.True(s.Expanded);
                Assert.Contains("chain of thought", s.DetailText);
            },
            (app, _) => app.InjectKey(Key.CursorLeft),
            (_, s) => Assert.False(s.Expanded));
    }

    [Fact(Timeout = 30000)]
    public async Task Tab_Moves_Focus_Between_The_Panels_And_The_Hints_Follow()
    {
        await Drive(FourSteps(),
            (_, s) => s.FocusSteps(),
            (_, s) => Assert.Contains("e/b errors", s.HintsText),   // steps hints
            (app, _) => app.InjectKey(Key.Tab),
            (_, s) => Assert.Contains("scroll", s.HintsText));      // flux hints
    }
}
