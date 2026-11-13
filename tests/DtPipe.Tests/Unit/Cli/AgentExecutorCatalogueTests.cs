using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using ModelContextProtocol.Server;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F1 — executor wiring: the LLM only ever sees the tools it is offered, and a call to any other is
/// refused where the loop reads it.
/// </summary>
public class AgentExecutorCatalogueTests
{
    private sealed class PlanAndExecuteTools
    {
        public int ValidateInvocations;
        public int ExecuteInvocations;

        [McpServerTool(Name = "validate-yaml-job")]
        [System.ComponentModel.Description("Validate a YAML job.")]
        public string ValidateYamlJob(string yamlContent)
        {
            ValidateInvocations++;
            return "{\"valid\":true}";
        }

        [McpServerTool(Name = "execute-yaml-job")]
        [WritesToTarget]
        [System.ComponentModel.Description("Execute a YAML job.")]
        public string ExecuteYamlJob(string yamlContent)
        {
            ExecuteInvocations++;
            return "{\"ran\":true}";
        }
    }

    /// <summary>Captures every tool definition list the LLM was offered during the run.</summary>
    private sealed class CapturingLlmClient : ILlmClient
    {
        private readonly HashSet<string> _seenToolNames = new(StringComparer.OrdinalIgnoreCase);
        public IReadOnlySet<string> SeenToolNames => _seenToolNames;

        public string ProviderName => "capturing";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools, int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            foreach (var t in tools)
                _seenToolNames.Add(t.Name);

            // Terminate after the first turn with a text reply so the loop completes.
            return Task.FromResult(new LlmResponse(new ChatMessage("assistant", "done planning"), true, null));
        }
    }

    /// <summary>
    /// Answers each call with the next scripted response and keeps the "tool" messages the model
    /// was handed on its last call, which is what it reads of its own tool calls.
    /// </summary>
    private sealed class ScriptedLlmClient : ILlmClient
    {
        private readonly Queue<LlmResponse> _responses;
        public List<ChatMessage> ToolMessagesSeen { get; private set; } = new();

        public ScriptedLlmClient(params LlmResponse[] responses) => _responses = new Queue<LlmResponse>(responses);

        public string ProviderName => "scripted";
        public Task<List<string>> ListModelsAsync(string baseUrl, CancellationToken ct = default) => Task.FromResult(new List<string>());

        public Task<LlmResponse> ChatAsync(string baseUrl, string model, List<ChatMessage> messages, List<ToolDefinition> tools, int maxTokens = 16384, double temperature = 0.7, int? seed = null, CancellationToken ct = default)
        {
            ToolMessagesSeen = messages.Where(m => m.Role == "tool").ToList();
            return Task.FromResult(_responses.Dequeue());
        }
    }

    /// <summary>
    /// A call naming an offered tool or not, with the arguments a model would send to write: the
    /// <c>apply</c> flag is set, so a call that reached the execution tool would write.
    /// </summary>
    private static LlmResponse CallsTo(params (string Id, string Name)[] calls) =>
        new(new ChatMessage("assistant", "calling", null,
            calls.Select(c => new ToolCall(c.Id, c.Name, JsonDocument.Parse("""{"yamlContent":"x","apply":true}""").RootElement)).ToList()),
            true, null);

    private static AgentExecutor BuildExecutor(PlanAndExecuteTools tools, ILlmClient llm)
        => new(new McpToolProvider(tools), llm);

    [Fact]
    public async Task The_Llm_Never_Sees_The_Execution_Tool()
    {
        var llm = new CapturingLlmClient();
        var executor = BuildExecutor(new PlanAndExecuteTools(), llm);

        await executor.TurnAsync("plan a pipeline", maxIterations: 3);

        Assert.DoesNotContain("execute-yaml-job", llm.SeenToolNames);
        Assert.Contains("validate-yaml-job", llm.SeenToolNames);
    }

    /// <summary>
    /// F1 at the call: the catalogue hides the execution tool, and a call to it by name is refused
    /// all the same, in either spelling, without reaching the provider. The refusal is the call's
    /// answer, so the conversation stays valid and the step stays in the trajectory.
    /// </summary>
    [Fact]
    public async Task A_Call_To_The_Hidden_Execution_Tool_Is_Refused_And_Never_Invoked()
    {
        var tools = new PlanAndExecuteTools();
        var llm = new ScriptedLlmClient(
            CallsTo(("1", "execute-yaml-job"), ("2", "execute_yaml_job")),
            new LlmResponse(new ChatMessage("assistant", "done planning"), true, null));
        var executor = BuildExecutor(tools, llm);

        await executor.TurnAsync("write it", maxIterations: 3);

        Assert.Equal(0, tools.ExecuteInvocations);
        Assert.Equal(new[] { "1", "2" }, llm.ToolMessagesSeen.Select(m => m.ToolCallId));
        foreach (var answer in llm.ToolMessagesSeen)
        {
            using var doc = JsonDocument.Parse(answer.Content!);
            Assert.Contains("execute", doc.RootElement.GetProperty("error").GetString());
            var offered = doc.RootElement.GetProperty("availableTools").EnumerateArray().Select(e => e.GetString()).ToList();
            Assert.DoesNotContain("execute-yaml-job", offered);
            Assert.Contains("validate-yaml-job", offered);
        }

        var steps = executor.Trajectory.Steps.Where(s => s.ToolName is not null).ToList();
        Assert.Equal(2, steps.Count);
        Assert.All(steps, s => Assert.True(s.IsError));
    }

    [Fact]
    public async Task An_Offered_Tool_Written_With_Underscores_Is_Still_Invoked()
    {
        var tools = new PlanAndExecuteTools();
        var llm = new ScriptedLlmClient(
            CallsTo(("1", "validate_yaml_job")),
            new LlmResponse(new ChatMessage("assistant", "done planning"), true, null));
        var executor = BuildExecutor(tools, llm);

        await executor.TurnAsync("check it", maxIterations: 3);

        Assert.Equal(1, tools.ValidateInvocations);
        Assert.False(executor.Trajectory.Steps.Single(s => s.ToolName is not null).IsError);
    }

    /// <summary>The system message is the role prompt, which forbids execution.</summary>
    [Fact]
    public async Task The_System_Prompt_Is_The_Planner_Prompt()
    {
        var llm = new CapturingLlmClient();
        var executor = BuildExecutor(new PlanAndExecuteTools(), llm);

        await executor.TurnAsync("plan", maxIterations: 3);

        var systemMsg = executor.Messages.First(m => m.Role == "system");
        Assert.Equal(AgentSystemPrompt.PlannerSystemPrompt, systemMsg.Content);
    }
}
