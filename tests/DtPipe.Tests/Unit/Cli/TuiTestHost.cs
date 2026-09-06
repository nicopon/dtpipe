using System;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Terminal.Gui.App;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Runs a single turn on the full-screen surface for the pre-session suites. Production only ever
/// runs the surface as a session (<see cref="TuiApp.RunSessionAsync"/>); this drives one turn
/// through that same entry point — a session whose worker runs the body once and returns — so the
/// E2–E4 suites keep asserting the surface without a single-turn form living in production for them.
/// </summary>
internal static class TuiTestHost
{
    /// <summary>
    /// Opens the surface, runs <paramref name="body"/> once with the session's own cancellation
    /// token, and closes it. The body's value comes back by side effect because
    /// <see cref="TuiApp.RunSessionAsync"/> returns <see cref="Task"/>, not <see cref="Task{T}"/>.
    /// </summary>
    public static async Task<T> RunOneTurnAsync<T>(
        this TuiApp app,
        TuiChrome chrome,
        TranscriptLog log,
        TuiTurnView view,
        AgentTrajectory trajectory,
        Func<(string Clock, string Meter)> header,
        Func<ITurnView, CancellationToken, Task<T>> body,
        CancellationToken ct,
        Action<IApplication>? surfaceReady = null,
        Action<TuiScreen>? onScreen = null)
    {
        T result = default!;
        await app.RunSessionAsync(chrome, log, view, trajectory, header,
            async surface => result = await body(view, surface.SessionToken),
            ct, surfaceReady, onScreen);
        return result;
    }
}
