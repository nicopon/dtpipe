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
/// Voie 4 §6 (suite) lot B1: "Execute this plan" runs the validated YAML straight through the
/// execution tool — never back through the model — passing <c>yamlContent</c> unchanged (F6). The
/// tool's own F2 guardrails then decide what actually happens.
/// </summary>
public class AgentExecutePlanTests
{
    private sealed class RecordingToolProvider : IAgentToolProvider
    {
        public string? LastToolName;
        public JsonElement LastArgs;
        public int Invocations;

        public List<ToolDefinition> GetToolDefinitions() => new();

        // Plan mode hides execute-yaml-job from the model — this test proves the human path does
        // not go through the model's allow-list.
        public List<ToolDefinition> GetToolDefinitions(AgentMode mode) => new();

        public Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
        {
            Invocations++;
            LastToolName = toolName;
            LastArgs = args.Clone();
            return Task.FromResult(ToolResult.Success("{\"success\":true,\"applied\":false,\"mode\":\"sample\"}"));
        }
    }

    private sealed class DeadLlmClient : ILlmClient
    {
        public string ProviderName => "dead";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());
        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools,
            int numCtx = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
            => Task.FromResult(new LlmResponse(new ChatMessage("assistant", "x"), true, null));
    }

    private static AgentExecutor Build(RecordingToolProvider tools)
    {
        IAnsiConsole console = AnsiConsole.Create(new AnsiConsoleSettings());
        return new AgentExecutor(tools, new DeadLlmClient(), new AgentTui(console), console);
    }

    [Fact]
    public async Task Executes_The_Validated_Yaml_Through_The_Execution_Tool_Unchanged()
    {
        var tools = new RecordingToolProvider();
        var executor = Build(tools);
        const string yaml = "main:\n  input: \"csv:in.csv\"\n  output: \"csv:out.csv\"\n";
        executor.Trajectory.LastGeneratedYaml = yaml;

        var result = await executor.ExecuteValidatedPlanAsync();

        Assert.Equal(1, tools.Invocations);
        Assert.Equal("execute-yaml-job", tools.LastToolName);
        Assert.Equal(yaml, tools.LastArgs.GetProperty("yamlContent").GetString());
        Assert.False(result.IsError);
    }

    [Fact]
    public async Task Refuses_When_There_Is_No_Validated_Plan()
    {
        var tools = new RecordingToolProvider();
        var executor = Build(tools);

        await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteValidatedPlanAsync());
        Assert.Equal(0, tools.Invocations);
    }

    private static string Render(string resultJson, bool isError)
    {
        var sw = new System.IO.StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(sw),
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Interactive = InteractionSupport.No,
        });
        new AgentTui(console).RenderExecutionResult(resultJson, isError);
        return sw.ToString();
    }

    [Fact]
    public void Renders_A_Dry_Run_As_Nothing_Written()
    {
        var json = "{\"success\":true,\"applied\":false,\"mode\":\"sample\",\"nextStep\":\"NOTHING WAS WRITTEN. Call again with apply=true.\"}";

        var output = Render(json, isError: false);

        Assert.Contains("Dry-run", output);
        Assert.Contains("NOTHING WAS WRITTEN", output);
    }

    [Fact]
    public void Renders_A_Real_Write_As_Data_Written()
    {
        var json = "{\"success\":true,\"applied\":true,\"mode\":\"write\",\"durationMs\":42}";

        var output = Render(json, isError: false);

        Assert.Contains("data written", output);
    }

    [Fact]
    public void Renders_A_Failure_With_Its_Errors()
    {
        var json = "{\"success\":false,\"stage\":\"execution\",\"applied\":false,\"errors\":[\"connection refused\"]}";

        var output = Render(json, isError: true);

        Assert.Contains("failed", output);
        Assert.Contains("connection refused", output);
    }
}
