using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Where one turn's step-by-step output goes. Two implementations: <see cref="ScrollbackTurnView"/>
/// writes each line to the terminal as today; <see cref="ShellTurnView"/> appends to the persistent
/// <see cref="AgentShell"/> frame and repaints. The planning loop calls this and never touches the
/// console directly, so the shell path is unit-tested without a live terminal (voie 4 §6 suite, D3).
/// </summary>
internal interface ITurnView
{
    /// <summary>Runs one streaming model call and renders its digest once the stream ends.</summary>
    Task<LlmResponse> StreamingStepAsync(int step, int maxSteps, AgentDetailLevel detail,
        Func<ILlmStreamObserver, Task<LlmResponse>> call);

    /// <summary>Runs one blocking model call (no token stream). The caller renders the digest after.</summary>
    Task<LlmResponse> BlockingStepAsync(int step, Func<CancellationToken, Task<LlmResponse>> call, CancellationToken ct);

    void Digest(int step, int maxSteps, TimeSpan elapsed, LlmResponse response, AgentDetailLevel detail);
    void ToolResult(string toolName, string result, bool isError);
    void AgentResponse(string content);
}

/// <summary>The pre-D3 behaviour: every line goes straight to the terminal scrollback.</summary>
internal sealed class ScrollbackTurnView : ITurnView
{
    private readonly IAnsiConsole _console;
    private readonly AgentTui _tui;

    public ScrollbackTurnView(IAnsiConsole console, AgentTui tui)
    {
        _console = console;
        _tui = tui;
    }

    public Task<LlmResponse> StreamingStepAsync(int step, int maxSteps, AgentDetailLevel detail,
        Func<ILlmStreamObserver, Task<LlmResponse>> call)
        => _tui.RunStreamingStepAsync(step, maxSteps, detail, call);

    public Task<LlmResponse> BlockingStepAsync(int step, Func<CancellationToken, Task<LlmResponse>> call, CancellationToken ct)
        => _console.Status()
            .Spinner(Spinner.Known.Dots)
            .SpinnerStyle(Style.Parse("blue bold"))
            .StartAsync($"Agent thinking (Step {step})...", _ => call(ct));

    public void Digest(int step, int maxSteps, TimeSpan elapsed, LlmResponse response, AgentDetailLevel detail)
        => _tui.RenderStepDigest(step, maxSteps, elapsed, response, detail);

    public void ToolResult(string toolName, string result, bool isError)
        => _tui.RenderToolResult(toolName, result, isError);

    public void AgentResponse(string content) => _tui.RenderAgentResponse(content);
}

/// <summary>
/// Appends the turn's output to the persistent shell frame. The streaming step feeds
/// <see cref="AgentShell.LiveTail"/> — no nested Live region — and its digest is committed to the
/// transcript once the stream ends; everything else is a transcript line.
/// </summary>
internal sealed class ShellTurnView : ITurnView
{
    private const int LiveTailLines = 12;

    private readonly AgentShell _shell;
    private readonly Action _repaint;

    public ShellTurnView(AgentShell shell, Action repaint)
    {
        _shell = shell;
        _repaint = repaint;
    }

    public async Task<LlmResponse> StreamingStepAsync(int step, int maxSteps, AgentDetailLevel detail,
        Func<ILlmStreamObserver, Task<LlmResponse>> call)
    {
        var view = new StreamingStepView(step, maxSteps);
        void Paint() { _shell.LiveTail = view.TailMarkup(LiveTailLines); _repaint(); }

        using var ticker = new Timer(_ => Paint(), null, 150, 150);
        LlmResponse result;
        try
        {
            result = await call(new LiveStreamObserver(view, Paint));
        }
        finally
        {
            _shell.LiveTail = null;
            _repaint();
        }

        AppendDigest(step, maxSteps, view.Elapsed, result, detail, view.ToolName);
        return result;
    }

    public async Task<LlmResponse> BlockingStepAsync(int step, Func<CancellationToken, Task<LlmResponse>> call, CancellationToken ct)
    {
        _shell.LiveTail = $"[grey]· Step {step} — thinking…[/]";
        _repaint();
        try
        {
            return await call(ct);
        }
        finally
        {
            _shell.LiveTail = null;
            _repaint();
        }
    }

    public void Digest(int step, int maxSteps, TimeSpan elapsed, LlmResponse response, AgentDetailLevel detail)
        => AppendDigest(step, maxSteps, elapsed, response, detail, null);

    private void AppendDigest(int step, int maxSteps, TimeSpan elapsed, LlmResponse response,
        AgentDetailLevel detail, string? toolFallback)
    {
        _shell.AppendRange(StepDigest.Lines(step, maxSteps, elapsed, response, toolFallback, detail));

        if (detail == AgentDetailLevel.Full && !string.IsNullOrWhiteSpace(response.Thinking))
            _shell.AppendRange(response.Thinking!.Replace("\r", string.Empty).Split('\n')
                .Select(l => $"  [grey35]{Markup.Escape(l)}[/]"));

        _repaint();
    }

    public void ToolResult(string toolName, string result, bool isError)
    {
        _shell.Append(StepDigest.ToolResultLine(toolName, result, isError));
        _repaint();
    }

    public void AgentResponse(string content)
    {
        if (string.IsNullOrWhiteSpace(content)) return;
        _shell.AppendRange(StepDigest.AgentResponseLines(content));
        _repaint();
    }
}
