using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The agent can stop to ask the user something. <c>ask-user</c> is a
/// turn terminator, not a tool — the loop never dispatches it, the turn ends on
/// <see cref="TurnOutcome.AwaitingUserInput"/> carrying the question, and the run exits non-zero
/// (an agent that stopped for want of an answer did not finish). Interactively the user answers
/// and the session continues; piped, it is a fail-closed 1.
/// </summary>
public class AgentClarificationTests
{
    private sealed class QueuedLlmClient : ILlmClient
    {
        private readonly IReadOnlyList<LlmResponse> _responses;
        private int _i;
        public QueuedLlmClient(params LlmResponse[] responses) => _responses = responses;
        public string ProviderName => "queued";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(_responses[Math.Min(_i++, _responses.Count - 1)]);
    }

    /// <summary>Records which tools it was asked to run — proves the non-ask-user calls still fire.</summary>
    private sealed class RecordingToolProvider : IAgentToolProvider
    {
        public List<string> Invoked { get; } = new();
        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();
        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            Invoked.Add(toolName);
            return Task.FromResult(ToolResult.Success("{}"));
        }
    }

    private static (AgentExecutor Executor, StringWriter Out, RecordingToolProvider Tools) Build(ILlmClient llm)
    {
        var sw = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(sw),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        console.Profile.Width = 100;
        var tools = new RecordingToolProvider();
        return (new AgentExecutor(tools, llm, new AgentTui(console), console), sw, tools);
    }

    private static LlmResponse AskUser(string question, string reasoning = "INTENT: I need one thing from the user")
    {
        var args = JsonSerializer.SerializeToElement(new { question });
        var calls = new List<ToolCall> { new("q1", "ask-user", args) };
        return new LlmResponse(new ChatMessage("assistant", reasoning, null, calls), true, null);
    }

    private static LlmResponse AskUserWithOptions(string question, params string[] options)
    {
        var args = JsonSerializer.SerializeToElement(new { question, options });
        var calls = new List<ToolCall> { new("q1", "ask-user", args) };
        return new LlmResponse(new ChatMessage("assistant", "INTENT: pick one", null, calls), true, null);
    }

    private static LlmResponse ToolThenAsk(string toolName, string question)
    {
        var calls = new List<ToolCall>
        {
            new("t1", toolName, JsonDocument.Parse("{}").RootElement.Clone()),
            new("q1", "ask-user", JsonSerializer.SerializeToElement(new { question })),
        };
        return new LlmResponse(new ChatMessage("assistant", "INTENT: inspect, then ask", null, calls), true, null);
    }

    [Fact]
    public async Task Ask_User_Ends_The_Turn_Awaiting_Input_And_Keeps_The_Question()
    {
        var (executor, _, _) = Build(new QueuedLlmClient(AskUser("What should the output file be called?")));

        int code = await executor.RunTurnAsync("anonymise the CSV", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(1, code);   // fail-closed: not a finished turn
        Assert.Equal(TurnOutcome.AwaitingUserInput, executor.LastTurnOutcome);
        Assert.Equal("What should the output file be called?", executor.PendingQuestion?.Text);
    }

    /// <summary>
    /// ask-user's schema declares an 'options' array — "a short list of choices, when the answer is
    /// a pick" — so a model that fills it answered the schema correctly. Reading only 'question'
    /// discarded that half before any surface could show it.
    /// </summary>
    [Fact]
    public async Task The_Choices_The_Model_Offered_Survive_The_Turn()
    {
        var (executor, _, _) = Build(new QueuedLlmClient(
            AskUserWithOptions("Which column keys the upsert?", "order_id", "customer_id")));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal("Which column keys the upsert?", executor.PendingQuestion?.Text);
        Assert.Equal(new[] { "order_id", "customer_id" }, executor.PendingQuestion?.Options);
    }

    /// <summary>The choices are numbered once, by the question itself, so the two surfaces cannot
    /// number them differently.</summary>
    [Fact]
    public void The_Choices_Are_Numbered_For_Display()
    {
        var question = new AgentQuestion("Which column?", new[] { "order_id", "customer_id" });

        Assert.Equal(
            new[] { "Which column?", "  1. order_id", "  2. customer_id" },
            question.Lines());
    }

    private static LlmResponse TextOnly(string content)
        => new(new ChatMessage("assistant", content), true, null);

    /// <summary>
    /// A weak model writes the ask-user call into its reply instead of emitting it. The turn then
    /// reported success while the agent waited for a reply nobody had been asked for — and the
    /// choices it offered went nowhere.
    /// </summary>
    [Fact]
    public async Task An_Ask_User_Call_Written_As_Text_Still_Ends_The_Turn_Awaiting_Input()
    {
        var (executor, _, _) = Build(new QueuedLlmClient(TextOnly(
            """{"question": "Which column keys the upsert?", "options": ["order_id", "customer_id"]}""")));

        int code = await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(1, code);
        Assert.Equal(TurnOutcome.AwaitingUserInput, executor.LastTurnOutcome);
        Assert.Equal("Which column keys the upsert?", executor.PendingQuestion?.Text);
        Assert.Equal(new[] { "order_id", "customer_id" }, executor.PendingQuestion?.Options);
    }

    /// <summary>The same call inside a fenced block, which is how a model that formats its reply
    /// writes it.</summary>
    [Fact]
    public async Task A_Fenced_Ask_User_Call_Is_Recognised_Too()
    {
        var (executor, _, _) = Build(new QueuedLlmClient(TextOnly(
            "```json\n{\"name\": \"ask-user\", \"arguments\": {\"question\": \"Which file?\"}}\n```")));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(TurnOutcome.AwaitingUserInput, executor.LastTurnOutcome);
        Assert.Equal("Which file?", executor.PendingQuestion?.Text);
    }

    /// <summary>
    /// A direct answer is a success at zero tools and zero plan — "which adapters do you have?"
    /// is answered, not asked. Only a reply that IS the call is reclassified; a reply that merely
    /// quotes one, or asks in prose, stays a finished turn.
    /// </summary>
    [Theory]
    [InlineData("dtpipe has 18 adapters: arrow, csv, duck…")]
    [InlineData("Here is an example call: {\"question\": \"what?\"} — you would send that as a tool call.")]
    [InlineData("**Question**\nQuel nom de fichier SQLite souhaitez-vous utiliser ?")]
    public async Task A_Reply_That_Is_Not_The_Call_Stays_A_Finished_Turn(string content)
    {
        var (executor, _, _) = Build(new QueuedLlmClient(TextOnly(content)));

        int code = await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(0, code);
        Assert.Equal(TurnOutcome.Succeeded, executor.LastTurnOutcome);
        Assert.Null(executor.PendingQuestion);
    }

    /// <summary>
    /// A surface picks a choice by the line the reader is standing on. The choices are the trailing
    /// lines, so a question that spans several lines still maps: counting forward from the top
    /// would misread every option after the first newline.
    /// </summary>
    [Fact]
    public void A_Line_Maps_Back_To_The_Choice_It_Shows()
    {
        var question = new AgentQuestion("Which column?\nThe upsert needs one.", new[] { "order_id", "customer_id" });

        Assert.Null(question.OptionAtLine(0));
        Assert.Null(question.OptionAtLine(1));
        Assert.Equal("order_id", question.OptionAtLine(2));
        Assert.Equal("customer_id", question.OptionAtLine(3));
        Assert.Null(question.OptionAtLine(4));
    }

    [Fact]
    public void A_Question_Without_Choices_Maps_No_Line()
        => Assert.Null(new AgentQuestion("Name the target file.").OptionAtLine(0));

    /// <summary>A question with no choices reads exactly as it did before they existed.</summary>
    [Fact]
    public void A_Question_Without_Choices_Is_Just_Its_Text()
        => Assert.Equal(new[] { "Name the target file." }, new AgentQuestion("Name the target file.").Lines());

    [Fact]
    public async Task The_Partial_Trajectory_Is_Kept()
    {
        var (executor, _, _) = Build(new QueuedLlmClient(AskUser("Which column holds the customer id?")));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Contains(executor.Trajectory.Steps, s => s.ToolName == "ask-user");
    }

    [Fact]
    public async Task Ask_User_Is_Never_Dispatched_As_A_Tool()
    {
        var (executor, _, tools) = Build(new QueuedLlmClient(AskUser("pick one")));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.DoesNotContain("ask-user", tools.Invoked);
    }

    [Fact]
    public async Task The_Underscore_Spelling_Is_Also_A_Terminator()
    {
        // A model that emits ask_user (underscore) instead of ask-user must still stop the turn,
        // not fall through to a "tool not found" it would then loop on.
        var args = JsonSerializer.SerializeToElement(new { question = "which id column?" });
        var calls = new List<ToolCall> { new("q1", "ask_user", args) };
        var response = new LlmResponse(new ChatMessage("assistant", "INTENT: ask", null, calls), true, null);
        var (executor, _, tools) = Build(new QueuedLlmClient(response));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(TurnOutcome.AwaitingUserInput, executor.LastTurnOutcome);
        Assert.Equal("which id column?", executor.PendingQuestion?.Text);
        Assert.DoesNotContain("ask_user", tools.Invoked);
    }

    [Fact]
    public async Task Other_Tool_Calls_In_The_Same_Turn_Still_Run()
    {
        var (executor, _, tools) = Build(new QueuedLlmClient(ToolThenAsk("inspect", "Which schema?")));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Contains("inspect", tools.Invoked);            // F5 preserved
        Assert.DoesNotContain("ask-user", tools.Invoked);
        Assert.Equal(TurnOutcome.AwaitingUserInput, executor.LastTurnOutcome);
    }

    [Fact]
    public async Task Awaiting_Input_Is_Distinct_From_An_Empty_Response()
    {
        var (executor, _, _) = Build(new QueuedLlmClient(AskUser("something", reasoning: "")));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        // Empty reasoning text must not make this read as EmptyResponse — the tool call is what matters.
        Assert.Equal(TurnOutcome.AwaitingUserInput, executor.LastTurnOutcome);
    }

    [Fact]
    public async Task The_Summary_Renders_The_Third_State()
    {
        var (executor, sw, _) = Build(new QueuedLlmClient(AskUser("What is the target table?")));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);

        var output = sw.ToString();
        Assert.Contains("AWAITING INPUT", output);
        Assert.Contains("asked you a question", output);
        Assert.Contains("What is the target table?", output);
        Assert.DoesNotContain("COMPLETED", output);
    }

    [Fact]
    public async Task An_Answer_On_The_Next_Turn_Clears_The_Pending_Question()
    {
        var (executor, _, _) = Build(new QueuedLlmClient(
            AskUser("Which id column?"),
            new LlmResponse(new ChatMessage("assistant", "Understood — here is the plan."), true, null)));

        await executor.RunTurnAsync("mission", "m", "http://x", new AgentOptions(), maxIterations: 5);
        Assert.Equal("Which id column?", executor.PendingQuestion?.Text);

        int code = await executor.RunTurnAsync("the 'client_ref' column", "m", "http://x", new AgentOptions(), maxIterations: 5);

        Assert.Equal(0, code);
        Assert.Equal(TurnOutcome.Succeeded, executor.LastTurnOutcome);
        Assert.Null(executor.PendingQuestion);
    }

    [Fact]
    public void Ask_User_Is_Offered_In_Every_Mode_Including_Plan()
    {
        var services = new ServiceCollection();
        services.AddSingleton<DtPipe.Core.Options.OptionsRegistry>();
        services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(Array.Empty<IStreamTransformerFactory>());
        services.AddSingleton<IEnumerable<IStreamReaderFactory>>(Array.Empty<IStreamReaderFactory>());
        services.AddSingleton<IEnumerable<IDataWriterFactory>>(Array.Empty<IDataWriterFactory>());
        services.AddSingleton<IMcpHelpService, McpHelpService>();
        var sp = services.BuildServiceProvider();
        var mcpTools = new DtPipeMcpTools(
            sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>(),
            Array.Empty<IDataTransformerFactory>(),
            sp.GetRequiredService<IEnumerable<IDataWriterFactory>>(),
            sp.GetRequiredService<IMcpHelpService>(),
            sp);
        var provider = new McpToolProvider(mcpTools);

        foreach (var mode in new[] { AgentMode.Plan, AgentMode.Execute, AgentMode.Autonomous })
            Assert.Contains(provider.GetToolDefinitions(mode), t => t.Name == "ask-user");
    }
}
