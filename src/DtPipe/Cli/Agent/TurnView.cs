using System;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;

namespace DtPipe.Cli.Agent;

/// <summary>
/// Where one turn's step-by-step output goes. <see cref="ScrollbackTurnView"/> writes each line to
/// the terminal, as the piped and CI paths still do; <see cref="DtPipe.Cli.Agent.Tui.TuiTurnView"/>
/// feeds the full-screen surface instead. The planning loop calls this and never touches the
/// console directly, so either path is unit-tested without a live terminal.
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

    /// <summary>
    /// A fresh plan YAML was captured from a <c>yamlContent</c> tool-call argument. The scrollback
    /// view ignores it; the full-screen surface feeds it to <see cref="PlanProgress"/>.
    /// </summary>
    void PlanUpdated(string yaml) { }
}

/// <summary>The piped / CI behaviour: every line goes straight to the terminal scrollback.</summary>
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
