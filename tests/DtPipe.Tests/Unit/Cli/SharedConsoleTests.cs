using System;
using System.IO;
using DtPipe.Cli.Infrastructure;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The DI console writes through a forwarder so
/// <see cref="Console.SetError"/> — how the agent's full-screen surface holds engine output back
/// while the toolkit owns the terminal — redirects Spectre too. The forwarder must not shift
/// Spectre's own capability detection, which keys off whether the writer <em>is</em> a standard
/// stream; the detection is pinned to a probe against the real stream, so it can't.
/// </summary>
[Collection("console-serial")]
public class SharedConsoleTests
{
    [Fact]
    public void A_Later_SetError_Redirects_The_Shared_Console()
    {
        var saved = Console.Error;
        var console = SharedConsole.Create();

        var buffer = new StringWriter();
        Console.SetError(buffer);
        try
        {
            console.Markup("[red]held back[/]");
        }
        finally
        {
            Console.SetError(saved);
        }

        Assert.Contains("held back", buffer.ToString());
    }

    [Fact]
    public void A_Console_Built_The_Old_Way_Ignores_SetError()
    {
        // The regression this lot fixes: the writer captured at construction, so a later
        // SetError has no effect on it.
        var saved = Console.Error;
        var captured = AnsiConsole.Create(new AnsiConsoleSettings { Out = new AnsiConsoleOutput(Console.Error) });

        var buffer = new StringWriter();
        Console.SetError(buffer);
        try
        {
            captured.Markup("[red]not held back[/]");
        }
        finally
        {
            Console.SetError(saved);
        }

        Assert.DoesNotContain("not held back", buffer.ToString());
    }

    [Fact]
    public void Capability_Detection_Matches_A_Probe_Against_The_Real_Stream()
    {
        var probe = AnsiConsole.Create(new AnsiConsoleSettings { Out = new AnsiConsoleOutput(Console.Error) });
        var shared = SharedConsole.Create();

        Assert.Equal(probe.Profile.Capabilities.Ansi, shared.Profile.Capabilities.Ansi);
        Assert.Equal(probe.Profile.Capabilities.Interactive, shared.Profile.Capabilities.Interactive);
        Assert.Equal(probe.Profile.Capabilities.ColorSystem, shared.Profile.Capabilities.ColorSystem);
    }
}
