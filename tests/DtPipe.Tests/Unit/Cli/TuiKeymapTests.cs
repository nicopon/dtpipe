using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Terminal.Gui.Drivers;
using Terminal.Gui.Input;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E2: the full-screen surface's keyboard contract, asserted without
/// starting an application. The Ctrl+C case is the one that matters most — the toolkit puts the
/// terminal in raw mode, so Ctrl+C is a keystroke and no SIGINT is raised; if this map stops
/// classifying it, an interrupted run reports success instead of 130 (F16).
/// </summary>
public class TuiKeymapTests
{
    [Fact]
    public void Ctrl_C_Is_A_Quit()
        => Assert.Equal(EditorSignal.Quit, TuiKeymap.Classify(Key.C.WithCtrl));

    [Fact]
    public void Esc_Is_An_Interrupt()
        => Assert.Equal(EditorSignal.Interrupt, TuiKeymap.Classify(Key.Esc));

    [Fact]
    public void Ctrl_O_Cycles_The_Detail_Level()
        => Assert.Equal(EditorSignal.CycleDetail, TuiKeymap.Classify(Key.O.WithCtrl));

    [Fact]
    public void Shift_Tab_Cycles_The_Mode()
        => Assert.Equal(EditorSignal.CycleMode, TuiKeymap.Classify(Key.Tab.WithShift));

    [Fact]
    public void Enter_Submits()
        => Assert.Equal(EditorSignal.Submit, TuiKeymap.Classify(Key.Enter));

    [Theory]
    [InlineData(KeyCode.A)]
    [InlineData(KeyCode.Z)]
    [InlineData(KeyCode.Space)]
    [InlineData(KeyCode.CursorDown)]
    public void Ordinary_Keys_Are_Not_Shortcuts(KeyCode code)
        => Assert.Equal(EditorSignal.None, TuiKeymap.Classify(code));

    [Fact]
    public void A_Bare_C_Is_Not_A_Quit()
    {
        // The modifier is the whole difference: typing "c" must never end the run.
        Assert.Equal(EditorSignal.None, TuiKeymap.Classify(Key.C));
    }

    [Fact]
    public void Both_Quit_And_Interrupt_End_The_Turn_For_Now()
    {
        // Until the soft cancel lands, Esc stops the run exactly like Ctrl+C.
        Assert.True(TuiKeymap.EndsTheTurn(EditorSignal.Quit));
        Assert.True(TuiKeymap.EndsTheTurn(EditorSignal.Interrupt));

        Assert.False(TuiKeymap.EndsTheTurn(EditorSignal.None));
        Assert.False(TuiKeymap.EndsTheTurn(EditorSignal.Submit));
        Assert.False(TuiKeymap.EndsTheTurn(EditorSignal.CycleDetail));
        Assert.False(TuiKeymap.EndsTheTurn(EditorSignal.CycleMode));
    }
}
