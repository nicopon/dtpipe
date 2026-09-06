using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using Spectre.Console;
using Terminal.Gui.Drivers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E4: the plan/DAG panel on the real full-screen surface. A plan YAML
/// captured mid-turn and the validate result that follows it flow through
/// <see cref="TuiTurnView.PlanSnapshot"/> and land in the panel — the branch line plus the badge.
/// </summary>
[Collection(TerminalGuiCollection.Name)]
public class TuiPlanPanelTests
{
    private static readonly TuiChrome Chrome = new("dtpipe agent · test", "plan · detail: compact");

    private static (string Clock, string Meter) Header() => ("3s", "42 tok");

    private const string PlanYaml = """
        main:
          input: "csv:in.csv"
          output: "csv:out.csv"
        """;

    private const string ValidateOk = "{\"success\":true,\"message\":\"YAML job configuration and topology are valid.\"}";

    [Fact(Timeout = 30000)]
    public async Task The_Plan_Panel_Shows_The_Branch_And_Its_Badge_After_A_Validated_Plan()
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
        var view = new TuiTurnView(log, new DagTopologyService(Array.Empty<IStreamTransformerFactory>()));
        var stop = new TaskCompletionSource();
        TuiScreen? screen = null;
        string planText = string.Empty;

        await app.RunOneTurnAsync(Chrome, log, view, new AgentTrajectory(), Header,
            async (turnView, _) =>
            {
                turnView.PlanUpdated(PlanYaml);
                turnView.ToolResult("validate-yaml-job", ValidateOk, isError: false);
                await stop.Task;
                return "ok";
            },
            CancellationToken.None,
            surfaceReady: live => live.AddTimeout(TimeSpan.FromMilliseconds(50), () =>
            {
                // The panel updates a repaint tick after the turn fed the view.
                var text = screen?.PlanText ?? string.Empty;
                if (text.Contains("✓ validated"))
                {
                    planText = text;
                    stop.TrySetResult();
                    return false;
                }
                return true;
            }),
            onScreen: s => screen = s);

        Assert.Contains("main: csv:in.csv → csv:out.csv", planText);
        Assert.Contains("✓ validated", planText);
    }
}
