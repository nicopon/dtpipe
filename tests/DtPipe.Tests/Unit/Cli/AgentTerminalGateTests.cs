using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E6: the door between the sequential scrollback path and the full-screen
/// surface. Since the default flipped — a real interactive terminal now gets the surface unless
/// <c>--no-tui</c> asks otherwise — this is the one place the flip's own logic lives, and it is the
/// riskiest line in the lot: get it wrong and either a CI pipe tries to draw a screen, or a real
/// terminal silently never gets one.
///
/// <para>
/// <see cref="AgentExecutor.CanOwnTerminal"/> and <see cref="AgentExecutor.WantsFullScreen"/> take
/// the redirection flags as parameters rather than reading <c>Console.IsInputRedirected</c> /
/// <c>Console.IsOutputRedirected</c> themselves — both are always true inside this test host (stdin
/// and stdout are pipes here), which would make the predicates return false unconditionally and
/// this whole suite untestable. Production passes the real properties from <c>AgentCommand</c>.
/// </para>
/// </summary>
public class AgentTerminalGateTests
{
    private sealed class StreamingClient : ILlmClient, IStreamingLlmClient
    {
        public string ProviderName => "streaming";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(new LlmResponse(new ChatMessage("assistant", "x"), true, null));
        public Task<LlmResponse> ChatStreamAsync(string baseUrl, string model, List<ChatMessage> messages,
            List<ToolDefinition> tools, ILlmStreamObserver observer, int numCtx = 16384,
            double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(new LlmResponse(new ChatMessage("assistant", "x"), true, null));
    }

    /// <summary>A client with no streaming capability — the blocking-only shape.</summary>
    private sealed class BlockingOnlyClient : ILlmClient
    {
        public string ProviderName => "blocking";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(new LlmResponse(new ChatMessage("assistant", "x"), true, null));
    }

    private static IAnsiConsole RealTerminal()
    {
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new System.IO.StringWriter()),
            Ansi = AnsiSupport.Yes,
            Interactive = InteractionSupport.Yes,
        });
        return console;
    }

    private static IAnsiConsole DumbTerminal()
    {
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(new System.IO.StringWriter()),
            Ansi = AnsiSupport.No,
            Interactive = InteractionSupport.No,
        });
        return console;
    }

    [Fact]
    public void A_Real_Interactive_Terminal_With_A_Streaming_Client_Can_Own_The_Terminal()
        => Assert.True(AgentExecutor.CanOwnTerminal(RealTerminal(), new StreamingClient(), new AgentOptions(), stdinRedirected: false));

    [Fact]
    public void A_Real_Interactive_Terminal_Wants_The_Full_Screen_Surface_By_Default()
    {
        // The flip E6 makes: no flag needed, a real terminal gets the surface.
        bool wants = AgentExecutor.WantsFullScreen(RealTerminal(), new StreamingClient(), new AgentOptions(),
            stdinRedirected: false, stdoutRedirected: false);

        Assert.True(wants);
    }

    [Fact]
    public void No_Tui_Opts_Out_Of_The_Surface_But_Not_Of_Streaming()
    {
        var opts = new AgentOptions { NoTui = true };

        // --no-tui only refuses the surface; the underlying "could this terminal host a live
        // view" fact (used for other decisions) is untouched.
        Assert.True(AgentExecutor.CanOwnTerminal(RealTerminal(), new StreamingClient(), opts, stdinRedirected: false));
        Assert.False(AgentExecutor.WantsFullScreen(RealTerminal(), new StreamingClient(), opts,
            stdinRedirected: false, stdoutRedirected: false));
    }

    [Fact]
    public void No_Stream_Refuses_Both_The_Shell_Capability_And_The_Surface()
    {
        var opts = new AgentOptions { NoStream = true };

        Assert.False(AgentExecutor.CanOwnTerminal(RealTerminal(), new StreamingClient(), opts, stdinRedirected: false));
        Assert.False(AgentExecutor.WantsFullScreen(RealTerminal(), new StreamingClient(), opts,
            stdinRedirected: false, stdoutRedirected: false));
    }

    [Fact]
    public void A_Non_Streaming_Client_Never_Gets_The_Surface()
        => Assert.False(AgentExecutor.WantsFullScreen(RealTerminal(), new BlockingOnlyClient(), new AgentOptions(),
            stdinRedirected: false, stdoutRedirected: false));

    [Fact]
    public void A_Dumb_Or_Non_Interactive_Console_Never_Gets_The_Surface()
        => Assert.False(AgentExecutor.WantsFullScreen(DumbTerminal(), new StreamingClient(), new AgentOptions(),
            stdinRedirected: false, stdoutRedirected: false));

    [Fact]
    public void Redirected_Stdin_Refuses_Both_The_Shell_Capability_And_The_Surface()
    {
        // A pipe or a redirect keeps the piped path — the non-regression proof CI runs against —
        // whether or not --no-tui was even mentioned.
        Assert.False(AgentExecutor.CanOwnTerminal(RealTerminal(), new StreamingClient(), new AgentOptions(), stdinRedirected: true));
        Assert.False(AgentExecutor.WantsFullScreen(RealTerminal(), new StreamingClient(), new AgentOptions(),
            stdinRedirected: true, stdoutRedirected: false));
    }

    [Fact]
    public void Redirected_Stdout_Refuses_The_Surface_Even_Though_Stdin_Is_A_Real_Terminal()
    {
        // The surface drives the terminal directly, so a captured run must stay sequential even
        // when nothing else about it looks piped (e.g. `dtpipe agent ... > log.txt` at a real tty).
        bool canOwn = AgentExecutor.CanOwnTerminal(RealTerminal(), new StreamingClient(), new AgentOptions(), stdinRedirected: false);
        bool wants = AgentExecutor.WantsFullScreen(RealTerminal(), new StreamingClient(), new AgentOptions(),
            stdinRedirected: false, stdoutRedirected: true);

        Assert.True(canOwn);     // the shell-capability fact is unaffected by stdout alone
        Assert.False(wants);     // but the surface itself refuses
    }
}
