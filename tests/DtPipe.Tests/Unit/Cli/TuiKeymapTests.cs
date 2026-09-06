using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Terminal.Gui.Drivers;
using Terminal.Gui.Input;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The full-screen surface's keyboard contract, asserted without
/// starting an application. <see cref="TuiKeymap"/> is the surface's one keyboard authority — every
/// shortcut it acts on is named here, including the navigation keys the steps panel reads. The
/// Ctrl+C case is the one that matters most — the toolkit puts the terminal in raw mode, so Ctrl+C
/// is a keystroke and no SIGINT is raised; if this map stops classifying it, an interrupted run
/// reports success instead of 130 (F16).
/// </summary>
public class TuiKeymapTests
{
    [Fact]
    public void Ctrl_C_Is_A_Quit()
        => Assert.Equal(SurfaceSignal.Quit, TuiKeymap.Classify(Key.C.WithCtrl));

    [Fact]
    public void Esc_Is_An_Interrupt()
        => Assert.Equal(SurfaceSignal.Interrupt, TuiKeymap.Classify(Key.Esc));

    [Fact]
    public void Shift_Tab_Cycles_The_Mode()
        => Assert.Equal(SurfaceSignal.CycleMode, TuiKeymap.Classify(Key.Tab.WithShift));

    [Fact]
    public void Plain_Tab_Moves_To_The_Next_Panel()
        => Assert.Equal(SurfaceSignal.FocusNext, TuiKeymap.Classify(Key.Tab));

    [Fact]
    public void Enter_Is_A_Submit()
        => Assert.Equal(SurfaceSignal.Submit, TuiKeymap.Classify(Key.Enter));

    [Fact]
    public void E_And_B_Are_Error_Navigation()
    {
        // Classify names the signal a key *could* carry, unconditionally; TuiScreen.OnKey stays the
        // arbiter of whether the focused panel wants it — e/b are letters on the input line and
        // error jumps on the steps panel. A context-aware classifier is the two-authorities defect
        // R4 closed.
        Assert.Equal(SurfaceSignal.NextError, TuiKeymap.Classify(Key.E));
        Assert.Equal(SurfaceSignal.PrevError, TuiKeymap.Classify(Key.B));
    }

    [Fact]
    public void The_Arrows_Expand_And_Collapse_The_Detail()
    {
        Assert.Equal(SurfaceSignal.Expand, TuiKeymap.Classify(Key.CursorRight));
        Assert.Equal(SurfaceSignal.Collapse, TuiKeymap.Classify(Key.CursorLeft));
    }

    [Theory]
    [InlineData(KeyCode.A)]
    [InlineData(KeyCode.Z)]
    [InlineData(KeyCode.Space)]
    [InlineData(KeyCode.CursorDown)]
    [InlineData(KeyCode.CursorUp)]
    public void Ordinary_Keys_Are_Not_Shortcuts(KeyCode code)
        => Assert.Equal(SurfaceSignal.None, TuiKeymap.Classify(code));

    [Fact]
    public void A_Bare_C_Is_Not_A_Quit()
    {
        // The modifier is the whole difference: typing "c" must never end the run.
        Assert.Equal(SurfaceSignal.None, TuiKeymap.Classify(Key.C));
    }
}
