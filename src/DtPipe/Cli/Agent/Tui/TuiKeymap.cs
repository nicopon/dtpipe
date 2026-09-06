using Terminal.Gui.Drivers;
using Terminal.Gui.Input;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>What a keypress means to the full-screen surface. <see cref="None"/> = not a shortcut.</summary>
internal enum SurfaceSignal
{
    /// <summary>Not a shortcut — an ordinary key, handled by whichever view has focus.</summary>
    None,

    /// <summary>Esc — a soft interrupt: cancel the model call in flight, keep the session.</summary>
    Interrupt,

    /// <summary>Ctrl+C — leave the agent.</summary>
    Quit,

    /// <summary>Shift+Tab — cycle the operating mode.</summary>
    CycleMode,

    /// <summary>Tab — move to the next panel in the focus ring.</summary>
    FocusNext,

    /// <summary>Enter — submit the input line, or expand the step detail while the steps panel has focus.</summary>
    Submit,

    /// <summary>e — select the next error step. Only acts while the steps panel has focus.</summary>
    NextError,

    /// <summary>b — select the previous error step. Only acts while the steps panel has focus.</summary>
    PrevError,

    /// <summary>Right — expand the step detail. Only acts while the steps panel has focus.</summary>
    Expand,

    /// <summary>Left — collapse the step detail. Only acts while the steps panel has focus.</summary>
    Collapse,
}

/// <summary>
/// Maps a Terminal.Gui key to the agent's shortcut vocabulary. Pure, so the whole keyboard
/// contract is asserted from a scripted key list without starting an application.
///
/// <para>
/// This is the surface's one keyboard authority: every shortcut it acts on is named here and no
/// view reads a raw <see cref="KeyCode"/>. A signal whose meaning depends on the focused panel —
/// <see cref="SurfaceSignal.NextError"/>, <see cref="SurfaceSignal.Expand"/> and the others marked
/// so — is still classified unconditionally; <see cref="TuiScreen"/> stays the arbiter of whether
/// the focused panel wants it or the key was just something the user typed. Do not fold that
/// context back in here (passing the focused view to <see cref="Classify"/>): a context-aware
/// classifier is what split the contract between this map and <see cref="TuiScreen"/> before, and
/// the two drifted.
/// </para>
///
/// <para>
/// Ctrl+C matters more here than anywhere else: the toolkit puts the terminal in raw mode, where
/// Ctrl+C arrives as an ordinary keystroke and no SIGINT is ever raised. Nothing translates it back
/// into the POSIX exit code 130 unless this map does, and the run would otherwise report success
/// after the user interrupted it (F16).
/// </para>
/// </summary>
internal static class TuiKeymap
{
    public static SurfaceSignal Classify(Key key)
    {
        if (key is null) return SurfaceSignal.None;

        if (key.KeyCode == (KeyCode.C | KeyCode.CtrlMask)) return SurfaceSignal.Quit;
        if (key.KeyCode == KeyCode.Esc) return SurfaceSignal.Interrupt;
        if (key.KeyCode == (KeyCode.Tab | KeyCode.ShiftMask)) return SurfaceSignal.CycleMode;
        if (key.KeyCode == KeyCode.Tab) return SurfaceSignal.FocusNext;
        if (key.KeyCode == KeyCode.Enter) return SurfaceSignal.Submit;
        if (key.KeyCode == KeyCode.CursorRight) return SurfaceSignal.Expand;
        if (key.KeyCode == KeyCode.CursorLeft) return SurfaceSignal.Collapse;
        if (key.KeyCode == KeyCode.E) return SurfaceSignal.NextError;
        if (key.KeyCode == KeyCode.B) return SurfaceSignal.PrevError;

        return SurfaceSignal.None;
    }

    /// <summary>
    /// Whether a signal ends the whole session. Only Ctrl+C does: leaving is a decision, and the
    /// caller turns it into exit 130. Esc stops the model call and keeps the session — so a soft
    /// stop must never reach the process token, or an interrupted turn would report the code
    /// reserved for a real interrupt (F16).
    /// </summary>
    public static bool EndsTheSession(SurfaceSignal signal)
        => signal is SurfaceSignal.Quit;
}
