using Terminal.Gui.Drivers;
using Terminal.Gui.Input;

namespace DtPipe.Cli.Agent.Tui;

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
    /// Whether a signal must end the turn now. Until the soft cancel lands, an interrupt is
    /// treated exactly like a quit: both stop the run, and the caller reports 130. Splitting them
    /// — Esc keeping the session alive — is the next lot's job, and only this predicate changes.
    /// </summary>
    public static bool EndsTheTurn(EditorSignal signal)
        => signal is EditorSignal.Quit or EditorSignal.Interrupt;
}
