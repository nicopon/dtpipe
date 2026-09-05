using System;
using System.Threading;

namespace DtPipe.Cli;

/// <summary>
/// Ambient flag: true while execution is inside a tool call the agent's LLM made, as opposed to a
/// human typing at the CLI. A terminal-capability check alone cannot tell the two apart: <c>dtpipe
/// agent</c>'s own TUI and the MCP tools it invokes in-process (<see cref="Agent.McpToolProvider"/>)
/// share the exact same real console and stdin, so <c>Console.IsInputRedirected</c> is false and
/// <c>IAnsiConsole.Profile.Capabilities.Interactive</c> is true in both cases. Only the caller knows
/// which one it is — hence an explicit scope rather than another capability probe.
///
/// Every console path that would otherwise block on a keypress (<c>Console.ReadKey</c>, a Spectre
/// <c>SelectionPrompt</c>) because the terminal *looks* interactive must check <see cref="IsSuppressed"/>
/// first. Without it, an LLM-driven tool call — nobody is watching for a prompt — hangs forever, and
/// the CLI's own timeout machinery never fires because nothing is wrong with the LLM call itself.
/// </summary>
public static class NonInteractiveGuard
{
    private static readonly AsyncLocal<bool> _suppressed = new();

    /// <summary>True while inside a <see cref="Suppress"/> scope on this logical call chain.</summary>
    public static bool IsSuppressed => _suppressed.Value;

    /// <summary>Suppresses blocking console prompts for the lifetime of the returned scope. Flows
    /// through <c>await</c> continuations (including ones on the thread pool) the same way
    /// <see cref="CancellationToken"/> does; it does not cross a fire-and-forget task that
    /// deliberately drops the execution context.</summary>
    public static IDisposable Suppress()
    {
        bool previous = _suppressed.Value;
        _suppressed.Value = true;
        return new Restorer(previous);
    }

    private sealed class Restorer : IDisposable
    {
        private readonly bool _previous;
        private bool _disposed;
        public Restorer(bool previous) => _previous = previous;
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _suppressed.Value = _previous;
        }
    }
}
