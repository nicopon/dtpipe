using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Agent.Tui;
using Spectre.Console;
using Terminal.Gui.App;
using Terminal.Gui.Drivers;
using Terminal.Gui.Input;
using Terminal.Gui.Testing;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The surface holds one application for the whole conversation. Turns
/// follow one another with the input line open in between, the post-mission menu is a set of typed
/// commands, and Esc stops a turn without ending the session.
///
/// <para>
/// Driven headless on the managed <c>dotnet</c> driver: a state machine on a repeating timeout runs
/// on the UI thread and types into the real input line, so Enter travels the same path a person's
/// keystroke does. Esc matters most here — it is the toolkit's own quit key, so a soft cancel that
/// forgets to claim the keystroke tears the application down instead of stopping one model call.
/// </para>
/// </summary>
[Collection(TerminalGuiCollection.Name)]
public class TuiSessionTests
{
    /// <summary>Answers each model call from a script; the count is read from the UI thread.</summary>
    private sealed class ScriptedClient : ILlmClient, IStreamingLlmClient
    {
        private readonly Func<int, CancellationToken, Task<LlmResponse>> _script;
        private int _calls;

        public ScriptedClient(Func<int, CancellationToken, Task<LlmResponse>> script) => _script = script;

        public int Calls => Volatile.Read(ref _calls);
        public string ProviderName => "scripted";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => _script(Interlocked.Increment(ref _calls), ct);

        public Task<LlmResponse> ChatStreamAsync(string baseUrl, string model, List<ChatMessage> messages,
            List<ToolDefinition> tools, ILlmStreamObserver observer, int maxTokens = 16384,
            double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => _script(Interlocked.Increment(ref _calls), ct);
    }

    /// <summary>Records every invocation, so a test can assert a write did or did not happen.</summary>
    private sealed class RecordingToolProvider : IAgentToolProvider
    {
        public List<string> Invoked { get; } = new();

        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();

        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            lock (Invoked) Invoked.Add(toolName);
            return Task.FromResult(ToolResult.Success("{\"applied\":true,\"rows\":5}"));
        }
    }

    private sealed class NoopToolProvider : IAgentToolProvider
    {
        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();
        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
            => Task.FromResult(ToolResult.Success("{}"));
    }

    private static LlmResponse Text(string content) => new(new ChatMessage("assistant", content), true, null);

    private static (IAnsiConsole Console, StringWriter Out) BuildConsole()
    {
        var writer = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(writer),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Profile.Width = 110;
        console.Profile.Height = 40;
        return (console, writer);
    }

    /// <summary>
    /// Runs the session and drives it from the UI thread: each entry waits for its condition, then
    /// acts, one action per tick so an injected key is processed before the next.
    /// </summary>
    private static Task<(int Exit, string Scrollback)> Drive(
        AgentExecutor executor, IAnsiConsole console, StringWriter output,
        params (Func<TuiScreen, bool> When, Action<IApplication, TuiScreen> Do)[] script)
        => Drive(executor, console, output, confirm: null, script);

    private static async Task<(int Exit, string Scrollback)> Drive(
        AgentExecutor executor, IAnsiConsole console, StringWriter output,
        Func<string, string, Task<bool>>? confirm,
        params (Func<TuiScreen, bool> When, Action<IApplication, TuiScreen> Do)[] script)
    {
        TuiScreen? screen = null;
        Exception? failure = null;

        var session = new TuiSession(console, new AgentTui(console), executor, DriverRegistry.Names.DOTNET, confirm);
        int exit = await session.RunAsync("mission", "m", "http://x",
            new AgentOptions { Apply = confirm is not null }, maxIterations: 5, CancellationToken.None,
            surfaceReady: live =>
            {
                int i = 0;
                live.AddTimeout(TimeSpan.FromMilliseconds(40), () =>
                {
                    if (screen is null || i >= script.Length) return true;
                    try
                    {
                        if (!script[i].When(screen)) return true;
                        script[i++].Do(live, screen);
                    }
                    catch (Exception ex)
                    {
                        failure = ex;
                        live.RequestStop();
                        return false;
                    }
                    return true;
                });
            },
            onScreen: s => screen = s);

        if (failure is not null) throw failure;
        return (exit, output.ToString());
    }

    private static void Submit(IApplication app, TuiScreen screen, string line)
    {
        screen.InputText = line;
        app.InjectKey(Key.Enter);
    }

    [Fact(Timeout = 60000)]
    public async Task Two_Turns_Run_In_One_Application_And_Slash_Quit_Leaves()
    {
        var client = new ScriptedClient((call, _) => Task.FromResult(Text($"answer {call}")));
        var (console, output) = BuildConsole();
        var executor = new AgentExecutor(new NoopToolProvider(), client, new AgentTui(console), console);

        var (exit, scrollback) = await Drive(executor, console, output,
            (s => client.Calls >= 1 && s.Accepting, (app, s) => Submit(app, s, "and now the second one")),
            (s => client.Calls >= 2 && s.Accepting, (app, s) => Submit(app, s, "/quit")));

        Assert.Equal(0, exit);
        Assert.Equal(2, client.Calls);

        // One transcript for the whole session, replayed once the terminal is back — and the
        // verdict table printed after it, never during.
        Assert.Contains("answer 1", scrollback);
        Assert.Contains("answer 2", scrollback);
        Assert.Contains("Session Status", scrollback);
        Assert.True(scrollback.IndexOf("answer 2", StringComparison.Ordinal)
            < scrollback.IndexOf("Session Status", StringComparison.Ordinal),
            "the verdict must follow the replayed transcript, not precede it");
    }

    [Fact(Timeout = 60000)]
    public async Task Esc_Stops_The_Turn_And_The_Session_Stays_Open()
    {
        // The first call hangs until the user gives up on it; the second answers at once. If Esc
        // were treated as a quit — the toolkit's default for that key — the session would end here
        // and the second turn would never happen.
        var client = new ScriptedClient(async (call, ct) =>
        {
            if (call == 1) { await Task.Delay(System.Threading.Timeout.Infinite, ct); return Text("unreachable"); }
            return Text("answer after the interrupt");
        });
        var (console, output) = BuildConsole();
        var executor = new AgentExecutor(new NoopToolProvider(), client, new AgentTui(console), console);

        var (exit, scrollback) = await Drive(executor, console, output,
            (s => client.Calls >= 1 && !s.Accepting, (app, _) => app.InjectKey(Key.Esc)),
            (s => s.Accepting, (app, s) =>
            {
                Assert.Equal(TurnOutcome.UserInterrupted, executor.LastTurnOutcome);
                Submit(app, s, "try again");
            }),
            (s => client.Calls >= 2 && s.Accepting, (app, s) => Submit(app, s, "/quit")));

        Assert.Equal(0, exit);                                   // the last turn succeeded
        Assert.Equal(TurnOutcome.Succeeded, executor.LastTurnOutcome);
        Assert.Contains("answer after the interrupt", scrollback);
        Assert.Contains("Interrupted", scrollback);              // the stopped turn left its mark
    }

    [Fact(Timeout = 60000)]
    public async Task Slash_Mode_Cycles_The_Operating_Mode_And_An_Unknown_Command_Never_Reaches_The_Model()
    {
        var client = new ScriptedClient((call, _) => Task.FromResult(Text($"answer {call}")));
        var (console, output) = BuildConsole();
        var executor = new AgentExecutor(new NoopToolProvider(), client, new AgentTui(console), console);

        var (exit, _) = await Drive(executor, console, output,
            (s => client.Calls >= 1 && s.Accepting, (app, s) => Submit(app, s, "/exce")),
            (s => s.StatusText.Contains("not a command"), (app, s) => Submit(app, s, "/mode")),
            (s => s.StatusText.Contains("Mode is now"), (app, s) => Submit(app, s, "/quit")));

        Assert.Equal(0, exit);
        Assert.Equal(1, client.Calls);                           // neither command spent a model call
        Assert.Equal(AgentMode.Execute, executor.Mode);          // plan → execute
    }

    [Theory(Timeout = 60000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Slash_Exec_With_Apply_Runs_The_Plan_Only_After_The_Confirmation(bool approve)
    {
        // The launch-time --apply consent is not the whole gate: the plan you just read is
        // confirmed before it writes. On this surface that confirmation is a modal, so the answer
        // is injected here rather than driven through one.
        var client = new ScriptedClient((call, _) => Task.FromResult(Text($"answer {call}")));
        var tools = new RecordingToolProvider();
        var (console, output) = BuildConsole();
        var executor = new AgentExecutor(tools, client, new AgentTui(console), console);
        executor.Trajectory.LastGeneratedYaml = "jobs:\n  main:\n    input: csv:in.csv\n";

        var (exit, _) = await Drive(executor, console, output,
            confirm: (_, _) => Task.FromResult(approve),
            (s => client.Calls >= 1 && s.Accepting, (app, s) => Submit(app, s, "/exec")),
            (s => s.StatusText.Contains("executed") || s.StatusText.Contains("Cancelled"),
                (app, s) => Submit(app, s, "/quit")));

        Assert.Equal(0, exit);
        if (approve)
            Assert.Contains("execute-yaml-job", tools.Invoked);
        else
            Assert.Empty(tools.Invoked);
    }

    [Fact(Timeout = 60000)]
    public async Task A_Question_From_The_Agent_Lands_Above_The_Input_Line_And_The_Answer_Is_The_Next_Turn()
    {
        // E7 gave the model a way to stop and ask; here is where the answer is typed. The question
        // shows in the status band — the line directly above the input — and the input takes focus.
        var askUser = new LlmResponse(
            new ChatMessage("assistant", "I need the target name", null, new List<ToolCall>
            {
                new("c1", "ask-user", JsonDocument.Parse("{\"question\":\"What should the output file be called?\"}").RootElement.Clone()),
            }), true, null);

        var client = new ScriptedClient((call, _) =>
            Task.FromResult(call == 1 ? askUser : Text("using invoices.csv, then")));
        var (console, output) = BuildConsole();
        var executor = new AgentExecutor(new NoopToolProvider(), client, new AgentTui(console), console);

        var (exit, scrollback) = await Drive(executor, console, output,
            (s => s.StatusText.Contains("AWAITING INPUT"), (app, s) =>
            {
                Assert.Contains("What should the output file be called?", s.StatusText);
                Assert.True(s.Accepting, "the input line must be open for the answer");
                Submit(app, s, "invoices.csv");
            }),
            (s => client.Calls >= 2 && s.Accepting, (app, s) => Submit(app, s, "/quit")));

        Assert.Equal(0, exit);                                   // the answered turn succeeded
        Assert.Equal(TurnOutcome.Succeeded, executor.LastTurnOutcome);
        Assert.Contains("using invoices.csv", scrollback);
    }

    [Fact(Timeout = 60000)]
    public async Task Ctrl_C_Ends_The_Whole_Session_So_The_Run_Can_Report_130()
    {
        // The other half of the key split. Raw mode swallows SIGINT, so Ctrl+C is a keystroke and
        // nothing else turns it back into an interrupt: if this stops working, leaving a session
        // mid-turn reports success instead of the POSIX 130 (F16).
        var client = new ScriptedClient(async (_, ct) =>
        {
            await Task.Delay(System.Threading.Timeout.Infinite, ct);
            return Text("unreachable");
        });
        var (console, output) = BuildConsole();
        var executor = new AgentExecutor(new NoopToolProvider(), client, new AgentTui(console), console);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Drive(executor, console, output,
            (s => client.Calls >= 1 && !s.Accepting, (app, _) => app.InjectKey(Key.C.WithCtrl))));
    }
}
