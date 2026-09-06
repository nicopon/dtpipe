using System;

namespace DtPipe.Cli.Agent;

/// <summary>What a line typed into the full-screen session's input means.</summary>
internal enum SessionCommandKind
{
    /// <summary>Blank input — nothing to do.</summary>
    Empty,

    /// <summary>Ordinary text: the prompt for the next turn.</summary>
    Prompt,

    /// <summary>Run the validated plan through the engine, without going back to the model.</summary>
    Execute,

    /// <summary>Advance the operating mode one step around the cycle.</summary>
    Mode,

    /// <summary>Write the validated plan's YAML to a file (<see cref="SessionCommand.Text"/> = path).</summary>
    Save,

    /// <summary>Move to the step list — on this surface, the steps and detail panels are the review.</summary>
    Review,

    /// <summary>Record what the human made of what just happened, into the session trace.</summary>
    Note,

    /// <summary>Leave the session.</summary>
    Quit,

    /// <summary>List the commands.</summary>
    Help,

    /// <summary>A slash word that is not a command.</summary>
    Unknown,
}

/// <summary>
/// The session's input line, parsed. Pure, so the whole command vocabulary is asserted from
/// strings without a terminal.
///
/// <para>
/// A word starting with <c>/</c> is never forwarded to the model, even when it matches nothing:
/// a typo would otherwise spend a model call on <c>/exce</c>. Everything else is a prompt.
/// </para>
/// </summary>
/// <param name="Kind">What the line asks for.</param>
/// <param name="Text">The prompt, a command's argument, or the unrecognised word.</param>
internal readonly record struct SessionCommand(SessionCommandKind Kind, string Text)
{
    /// <summary>The commands, as the surface advertises them.</summary>
    public const string Hint = "/exec · /mode · /save · /note · /review · /quit";

    public static SessionCommand Parse(string? line)
    {
        var text = (line ?? string.Empty).Trim();
        if (text.Length == 0) return new SessionCommand(SessionCommandKind.Empty, string.Empty);
        if (text[0] != '/') return new SessionCommand(SessionCommandKind.Prompt, text);

        int space = text.IndexOf(' ');
        string verb = (space < 0 ? text[1..] : text[1..space]).ToLowerInvariant();
        string argument = space < 0 ? string.Empty : text[(space + 1)..].Trim();

        var kind = verb switch
        {
            "exec" or "execute" or "run" => SessionCommandKind.Execute,
            "mode" => SessionCommandKind.Mode,
            "save" => SessionCommandKind.Save,
            "note" => SessionCommandKind.Note,
            "review" or "steps" => SessionCommandKind.Review,
            "quit" or "exit" or "q" => SessionCommandKind.Quit,
            "help" or "?" => SessionCommandKind.Help,
            _ => SessionCommandKind.Unknown,
        };

        return new SessionCommand(kind, kind == SessionCommandKind.Unknown ? verb : argument);
    }
}
