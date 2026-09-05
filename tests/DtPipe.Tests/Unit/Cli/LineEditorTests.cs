using System;
using System.Linq;
using DtPipe.Cli.Agent;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite) lot D1: the shell's input line and its in-loop shortcuts are a pure state
/// machine — a scripted key sequence yields a deterministic text and signal.
/// </summary>
public class LineEditorTests
{
    private static ConsoleKeyInfo Ch(char c) => new(c, ToKey(c), shift: char.IsUpper(c), alt: false, control: false);
    private static ConsoleKeyInfo K(ConsoleKey k, bool ctrl = false, bool shift = false)
        => new('\0', k, shift, alt: false, ctrl);

    private static ConsoleKey ToKey(char c) => char.IsLetter(c)
        ? Enum.Parse<ConsoleKey>(char.ToUpperInvariant(c).ToString())
        : ConsoleKey.Spacebar;

    private static (string text, EditorSignal last) Type(string s)
    {
        var ed = new LineEditor();
        EditorSignal last = EditorSignal.None;
        foreach (var c in s) last = ed.Apply(Ch(c));
        return (ed.Text, last);
    }

    [Fact]
    public void Typing_Accumulates_Text()
    {
        var (text, sig) = Type("inspect csv");
        Assert.Equal("inspect csv", text);
        Assert.Equal(EditorSignal.None, sig);
    }

    [Fact]
    public void Backspace_And_Cursor_Movement_Edit_In_Place()
    {
        var ed = new LineEditor();
        foreach (var c in "abcd") ed.Apply(Ch(c));
        ed.Apply(K(ConsoleKey.LeftArrow));
        ed.Apply(K(ConsoleKey.LeftArrow));
        ed.Apply(K(ConsoleKey.Backspace));   // remove 'b'
        Assert.Equal("acd", ed.Text);
        Assert.Equal(1, ed.Cursor);

        ed.Apply(K(ConsoleKey.Home));
        ed.Apply(K(ConsoleKey.Delete));      // remove 'a'
        Assert.Equal("cd", ed.Text);

        ed.Apply(K(ConsoleKey.End));
        Assert.Equal(2, ed.Cursor);
    }

    [Fact]
    public void Enter_Submits()
    {
        var ed = new LineEditor();
        ed.Apply(Ch('h')); ed.Apply(Ch('i'));
        Assert.Equal(EditorSignal.Submit, ed.Apply(K(ConsoleKey.Enter)));
        Assert.Equal("hi", ed.Text);
    }

    [Theory]
    [InlineData(ConsoleKey.Escape, false, false, EditorSignal.Interrupt)]
    [InlineData(ConsoleKey.C, true, false, EditorSignal.Quit)]
    [InlineData(ConsoleKey.O, true, false, EditorSignal.CycleDetail)]
    [InlineData(ConsoleKey.Tab, false, true, EditorSignal.CycleMode)]
    public void Control_Keys_Are_Classified(ConsoleKey key, bool ctrl, bool shift, EditorSignal expected)
    {
        var ed = new LineEditor();
        Assert.Equal(expected, ed.Apply(K(key, ctrl, shift)));
    }

    [Fact]
    public void Control_Chords_Do_Not_Leak_Into_The_Text()
    {
        var ed = new LineEditor();
        ed.Apply(Ch('x'));
        ed.Apply(K(ConsoleKey.O, ctrl: true));
        ed.Apply(K(ConsoleKey.Tab, shift: true));
        ed.Apply(Ch('y'));
        Assert.Equal("xy", ed.Text);
    }

    [Fact]
    public void Reset_And_Set_Manage_The_Buffer()
    {
        var ed = new LineEditor();
        ed.Set("prefilled");
        Assert.Equal("prefilled", ed.Text);
        Assert.Equal(9, ed.Cursor);

        ed.Reset();
        Assert.True(ed.IsEmpty);
        Assert.Equal(0, ed.Cursor);
    }

    [Fact]
    public void RenderMarkup_Shows_The_Prompt_And_Escapes_The_Text()
    {
        var ed = new LineEditor();
        ed.Set("a [b] c");
        var markup = ed.RenderMarkup();
        Assert.Contains("›", markup);
        Assert.Contains("[[b]]", markup);   // Spectre-escaped brackets
    }
}
