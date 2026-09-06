using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// R2: running the validated plan is one policy — present it, ask again only when the launch
/// already consented to a real write (<c>--apply</c>), then hand the YAML to the engine. Both
/// surfaces call <see cref="PlanExecution.RunAsync"/>, so this pins what it decides and the two
/// cannot drift on when the write consent is even asked for.
/// </summary>
public class PlanExecutionTests
{
    private sealed class StubLlm : ILlmClient
    {
        public string ProviderName => "stub";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(new LlmResponse(new ChatMessage("assistant", "done"), true, null));
    }

    private sealed class RecordingTools : IAgentToolProvider
    {
        private readonly Func<ToolResult> _result;
        public RecordingTools(Func<ToolResult>? result = null)
            => _result = result ?? (() => ToolResult.Success("{\"applied\":true}"));

        public List<string> Invoked { get; } = new();
        public List<ToolDefinition> GetToolDefinitions() => new();
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();

        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            Invoked.Add(toolName);
            return Task.FromResult(_result());
        }
    }

    private const string Yaml = "jobs:\n  main:\n    input: csv:in.csv\n    output: csv:out.csv\n";

    private static AgentExecutor Build(IAgentToolProvider tools, string? plan = null)
    {
        IAnsiConsole console = AnsiConsole.Create(new AnsiConsoleSettings());
        var executor = new AgentExecutor(tools, new StubLlm(), new AgentTui(console), console);
        executor.Trajectory.LastGeneratedYaml = plan;
        return executor;
    }

    private static Func<string, Task> RecordPresent(List<string> log)
        => _ => { log.Add("present"); return Task.CompletedTask; };

    private static Func<string, Task<bool>> RecordConfirm(List<string> log, bool answer)
        => _ => { log.Add("confirm"); return Task.FromResult(answer); };

    [Fact]
    public async Task With_No_Validated_Plan_It_Returns_Null_And_Touches_Nothing()
    {
        var tools = new RecordingTools();
        var log = new List<string>();

        var result = await PlanExecution.RunAsync(Build(tools, plan: null), new AgentOptions { Apply = true },
            RecordPresent(log), RecordConfirm(log, true), CancellationToken.None);

        Assert.Null(result);
        Assert.Empty(tools.Invoked);
        Assert.Empty(log);
    }

    [Fact]
    public async Task A_Dry_Run_Is_Presented_Then_Executed_With_No_Confirmation()
    {
        var tools = new RecordingTools();
        var log = new List<string>();

        var result = await PlanExecution.RunAsync(Build(tools, Yaml), new AgentOptions { Apply = false },
            RecordPresent(log), RecordConfirm(log, false), CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(new[] { "present" }, log);               // confirm is never posed for a dry-run
        Assert.Equal(new[] { "execute-yaml-job" }, tools.Invoked);
    }

    [Fact]
    public async Task With_Apply_A_Refusal_Runs_Nothing_And_Presents_Before_Asking()
    {
        var tools = new RecordingTools();
        var log = new List<string>();

        var result = await PlanExecution.RunAsync(Build(tools, Yaml), new AgentOptions { Apply = true },
            RecordPresent(log), RecordConfirm(log, false), CancellationToken.None);

        Assert.Null(result);
        Assert.Empty(tools.Invoked);
        Assert.Equal(new[] { "present", "confirm" }, log);    // show what will run, then ask
    }

    [Fact]
    public async Task With_Apply_An_Approval_Runs_The_Plan_Once()
    {
        var tools = new RecordingTools();
        var log = new List<string>();

        var result = await PlanExecution.RunAsync(Build(tools, Yaml), new AgentOptions { Apply = true },
            RecordPresent(log), RecordConfirm(log, true), CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(new[] { "present", "confirm" }, log);
        Assert.Equal(new[] { "execute-yaml-job" }, tools.Invoked);
    }

    [Fact]
    public async Task A_Tool_Exception_Comes_Back_As_An_Error_Result_Not_A_Throw()
    {
        var tools = new RecordingTools(() => throw new InvalidOperationException("boom"));

        var result = await PlanExecution.RunAsync(Build(tools, Yaml), new AgentOptions { Apply = false },
            _ => Task.CompletedTask, _ => Task.FromResult(true), CancellationToken.None);

        Assert.NotNull(result);
        Assert.True(result!.IsError);
        Assert.Contains("boom", result.Content);
    }
}
