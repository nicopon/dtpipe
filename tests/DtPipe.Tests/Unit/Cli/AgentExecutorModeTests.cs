using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using ModelContextProtocol.Server;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F1 — executor wiring: the LLM only ever sees the tools allowed for the operating mode.
/// </summary>
public class AgentExecutorModeTests
{
    private sealed class PlanAndExecuteTools
       {
         [McpServerTool(Name = "validate-yaml-job")]
          [System.ComponentModel.Description("Validate a YAML job.")]
         public string ValidateYamlJob(string yamlContent) => "ok";

        [McpServerTool(Name = "execute-yaml-job")]
         [System.ComponentModel.Description("Execute a YAML job.")]
        public string ExecuteYamlJob(string yamlContent) => "ran";
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

      private static AgentExecutor BuildExecutor(PlanAndExecuteTools tools, CapturingLlmClient llm)
          {
         IAnsiConsole console = Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings());
         var tui = new AgentTui(console);
         var provider = new McpToolProvider(tools);
         return new AgentExecutor(provider, llm, tui, console);
           }

       [Fact]
    public async Task In_Plan_Mode_The_Llm_Never_Sees_The_Execution_Tool()
          {
         var llm = new CapturingLlmClient();
         var executor = BuildExecutor(new PlanAndExecuteTools(), llm);

         var options = new AgentOptions { Mode = AgentMode.Plan };
         await executor.RunTurnAsync("plan a pipeline", "m", "http://localhost:11434", options, maxIterations: 3);

         Assert.DoesNotContain("execute-yaml-job", llm.SeenToolNames);
         Assert.Contains("validate-yaml-job", llm.SeenToolNames);
           }

        [Fact]
    public async Task In_Execute_Mode_The_Llm_Sees_The_Execution_Tool()
             {
            var llm = new CapturingLlmClient();
             var executor = BuildExecutor(new PlanAndExecuteTools(), llm);

             var options = new AgentOptions { Mode = AgentMode.Execute };
             await executor.RunTurnAsync("run a pipeline", "m", "http://localhost:11434", options, maxIterations: 3);

            Assert.Contains("execute-yaml-job", llm.SeenToolNames);
              }

        /// <summary>
         /// Verifies the system prompt injected into the LLM conversation reflects the operating mode
         /// (F1): the planner prompt forbids execution.
         /// </summary>
       [Fact]
    public async Task Plan_Mode_Injects_The_Planner_System_Prompt()
        {
         var llm = new CapturingLlmClient();
         var executor = BuildExecutor(new PlanAndExecuteTools(), llm);
         var options = new AgentOptions { Mode = AgentMode.Plan };

         await executor.RunTurnAsync("plan", "m", "http://localhost:11434", options, maxIterations: 3);

         var systemMsg = executor.Messages.First(m => m.Role == "system");
         Assert.Contains("PLANNER", systemMsg.Content);
           }

       // ── The operating mode is a live session state ──

       [Theory]
       [InlineData(AgentMode.Plan, AgentMode.Execute)]
       [InlineData(AgentMode.Execute, AgentMode.Autonomous)]
       [InlineData(AgentMode.Autonomous, AgentMode.Plan)]
    public void CycleMode_Walks_Plan_Execute_Autonomous_And_Wraps(AgentMode from, AgentMode expected)
        {
         Assert.Equal(expected, AgentExecutor.NextMode(from));
           }

       [Fact]
    public async Task Switching_From_Plan_To_Execute_Between_Turns_Unlocks_The_Execution_Tool()
        {
         var llm = new CapturingLlmClient();
         var executor = BuildExecutor(new PlanAndExecuteTools(), llm);
         var options = new AgentOptions { Mode = AgentMode.Plan };

         await executor.RunTurnAsync("plan a pipeline", "m", "http://localhost:11434", options, maxIterations: 3);
         Assert.DoesNotContain("execute-yaml-job", llm.SeenToolNames);

         executor.CycleMode();
         Assert.Equal(AgentMode.Execute, executor.Mode);

         await executor.RunTurnAsync("now run it", "m", "http://localhost:11434", options, maxIterations: 3);

         Assert.Contains("execute-yaml-job", llm.SeenToolNames);
         var systemMsg = executor.Messages.First(m => m.Role == "system");
         Assert.Contains("EXECUTOR", systemMsg.Content);
           }

       [Fact]
    public async Task The_Session_Mode_Is_Authoritative_Once_Seeded()
        {
         // A later turn passing a different options.Mode must not silently override a mode the
         // user cycled — the executor's Mode is the single authority after the first turn.
         var llm = new CapturingLlmClient();
         var executor = BuildExecutor(new PlanAndExecuteTools(), llm);

         await executor.RunTurnAsync("plan", "m", "http://localhost:11434", new AgentOptions { Mode = AgentMode.Plan }, maxIterations: 3);
         Assert.Equal(AgentMode.Plan, executor.Mode);

         await executor.RunTurnAsync("again", "m", "http://localhost:11434", new AgentOptions { Mode = AgentMode.Execute }, maxIterations: 3);

         Assert.Equal(AgentMode.Plan, executor.Mode);
         Assert.DoesNotContain("execute-yaml-job", llm.SeenToolNames);
           }
}
