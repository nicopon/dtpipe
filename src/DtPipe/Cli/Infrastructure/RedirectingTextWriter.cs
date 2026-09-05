using System;
using System.IO;
using System.Text;
using Spectre.Console;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Builds the shared <see cref="IAnsiConsole"/>. It detects the terminal once against the real
/// <c>Console.Error</c>, then writes through a <see cref="RedirectingTextWriter"/> so that a later
/// <see cref="Console.SetError"/> — the agent's full-screen surface holding engine output back
/// while the toolkit owns the terminal — redirects every Spectre write too, not just direct
/// <c>Console.Error</c> calls. Detection is pinned to what the real stream reported, so the wrapper
/// cannot shift it: Spectre keys ANSI/colour off whether the writer <em>is</em> a standard stream,
/// and a wrapped one no longer is.
/// </summary>
internal static class SharedConsole
{
    public static IAnsiConsole Create()
    {
        var probe = AnsiConsole.Create(new AnsiConsoleSettings { Out = new AnsiConsoleOutput(Console.Error) });
        var caps = probe.Profile.Capabilities;

        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new RedirectingTextWriter(() => Console.Error)),
            Ansi = caps.Ansi ? AnsiSupport.Yes : AnsiSupport.No,
            ColorSystem = caps.ColorSystem switch
            {
                ColorSystem.NoColors => ColorSystemSupport.NoColors,
                ColorSystem.Legacy => ColorSystemSupport.Legacy,
                ColorSystem.Standard => ColorSystemSupport.Standard,
                ColorSystem.EightBit => ColorSystemSupport.EightBit,
                ColorSystem.TrueColor => ColorSystemSupport.TrueColor,
                _ => ColorSystemSupport.Detect,
            },
            Interactive = caps.Interactive ? InteractionSupport.Yes : InteractionSupport.No,
        });

        // Width stays live — Spectre reads System.Console.BufferWidth on each render regardless of
        // the writer — so nothing to carry over here.
        return console;
    }
}

/// <summary>
/// A <see cref="TextWriter"/> that resolves its real destination on every call instead of capturing
/// it once. See <see cref="SharedConsole"/> for why the DI console needs it.
/// </summary>
internal sealed class RedirectingTextWriter : TextWriter
{
    private readonly Func<TextWriter> _target;

    public RedirectingTextWriter(Func<TextWriter> target) => _target = target;

    public override Encoding Encoding => _target().Encoding;

    public override void Write(char value) => _target().Write(value);
    public override void Write(string? value) => _target().Write(value);
    public override void Write(char[] buffer, int index, int count) => _target().Write(buffer, index, count);
    public override void Write(ReadOnlySpan<char> buffer) => _target().Write(buffer);

    public override void Flush() => _target().Flush();
}
