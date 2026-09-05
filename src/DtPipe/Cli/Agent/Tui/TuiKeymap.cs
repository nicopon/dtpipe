using Terminal.Gui.Drivers;
using Terminal.Gui.Input;

namespace DtPipe.Cli.Agent.Tui;

/// <summary>What a keypress means to the full-screen surface. <see cref="None"/> = not a shortcut.</summary>
internal enum EditorSignal
{
    /// <summary>Not a shortcut — an ordinary key, handled by whichever view has focus.</summary>
    None,

    /// <summary>Enter — the input line's text is complete.</summary>
    Submit,

    /// <summary>Esc — a soft interrupt: cancel the model call in flight, keep the session.</summary>
    Interrupt,

    /// <summary>Ctrl+C — leave the agent.</summary>
    Quit,

    /// <summary>Ctrl+O — cycle the scrollback detail level.</summary>
    CycleDetail,

    /// <summary>Shift+Tab — cycle the operating mode.</summary>
    CycleMode,
}

/// <summary>
/// Maps a Terminal.Gui key to the agent's shortcut vocabulary. Pure, so the whole keyboard
/// contract is asserted from a scripted key list without starting an application.
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
    /// <summary>What a keypress means to the agent shell. <see cref="EditorSignal.None"/> = not a shortcut.</summary>
    public static EditorSignal Classify(Key key)
    {
        if (key is null) return EditorSignal.None;

        if (key.KeyCode == (KeyCode.C | KeyCode.CtrlMask)) return EditorSignal.Quit;
        if (key.KeyCode == KeyCode.Esc) return EditorSignal.Interrupt;
        if (key.KeyCode == (KeyCode.O | KeyCode.CtrlMask)) return EditorSignal.CycleDetail;
        if (key.KeyCode == (KeyCode.Tab | KeyCode.ShiftMask)) return EditorSignal.CycleMode;
        if (key.KeyCode == KeyCode.Enter) return EditorSignal.Submit;

        return EditorSignal.None;
    }

    /// <summary>
    /// Whether a signal stops the model call in flight. Both do — the difference is what survives,
    /// which <see cref="EndsTheSession"/> decides.
    /// </summary>
    public static bool EndsTheTurn(EditorSignal signal)
        => signal is EditorSignal.Quit or EditorSignal.Interrupt;

    /// <summary>
    /// Whether a signal ends the whole session. Only Ctrl+C does: leaving is a decision, and the
    /// caller turns it into exit 130. Esc stops the model call and keeps the session — so a soft
    /// stop must never reach the process token, or an interrupted turn would report the code
    /// reserved for a real interrupt (F16).
    /// </summary>
    public static bool EndsTheSession(EditorSignal signal)
        => signal is EditorSignal.Quit;

}
