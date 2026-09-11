using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using DtPipe.Tests.Helpers;
using ModelContextProtocol.Server;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F1 — planner/executor split: in PLAN mode the execution tool is never offered to the LLM, and
/// the planner prompt is selected; execution stays a deterministic engine step.
/// </summary>
public class AgentModeTests
{
     /// <summary>
       /// A tools class that mirrors the real MCP surface: a planning tool and the execution tool
       /// '<c>execute-yaml-job</c>' that must be filtered out in PLAN mode.
       /// </summary>
    private sealed class PlanAndExecuteTools
      {
         [McpServerTool(Name = "validate-yaml-job")]
         [System.ComponentModel.Description("Validate a YAML job.")]
         public string ValidateYamlJob(string yamlContent) => "ok";

        [McpServerTool(Name = "execute-yaml-job")]
        [WritesToTarget]
        [System.ComponentModel.Description("Execute a YAML job.")]
        public string ExecuteYamlJob(string yamlContent) => "ran";

         // Named nothing like the execution tool on purpose: the filter must follow the mark, not
         // a spelling. A name-matching filter lets a second writing tool through unnoticed.
        [McpServerTool(Name = "push-to-warehouse")]
        [WritesToTarget]
        [System.ComponentModel.Description("Write rows somewhere.")]
        public string PushToWarehouse(string yamlContent) => "pushed";
        }

     [Fact]
    public void Plan_Mode_Excludes_Execution_Tool_But_KeePs_Planning_Tools()
      {
        var provider = new McpToolProvider(new PlanAndExecuteTools());

        var planTools = provider.GetToolDefinitions(AgentMode.Plan);
        Assert.DoesNotContain(planTools, t => t.Name == "execute-yaml-job");
        Assert.DoesNotContain(planTools, t => t.Name == "push-to-warehouse");
        Assert.Contains(planTools, t => t.Name == "validate-yaml-job");
        }

      [Fact]
    public void Execute_Mode_Includes_Execution_Tool()
      {
        var provider = new McpToolProvider(new PlanAndExecuteTools());

        var execTools = provider.GetToolDefinitions(AgentMode.Execute);
        Assert.Contains(execTools, t => t.Name == "execute-yaml-job");
        }

      [Fact]
    public void Autonomous_Mode_Includes_Execution_Tool()
      {
        var provider = new McpToolProvider(new PlanAndExecuteTools());

        var autoTools = provider.GetToolDefinitions(AgentMode.Autonomous);
        Assert.Contains(autoTools, t => t.Name == "execute-yaml-job");
        }

     [Fact]
    public void ToolModePolicy_Blocks_Execution_Tool_In_Plan_Mode_Only()
        {
            // 'execute-yaml-job' is the only execution tool and must be blocked in PLAN mode.
        Assert.True(ToolModePolicy.IsBlockedInPlanMode("execute-yaml-job"));
        Assert.True(ToolModePolicy.IsExecutionTool("execute-yaml-job"));
         // A planning tool is never blocked.
        Assert.False(ToolModePolicy.IsBlockedInPlanMode("validate-yaml-job"));
        Assert.False(ToolModePolicy.IsExecutionTool("validate-yaml-job"));
          }

       [Fact]
    public void Select_Plan_Mode_Uses_Planner_Prompt_Forbidding_Execution()
         {
        var plan = AgentSystemPrompt.Select(AgentMode.Plan);
        Assert.Contains("PLANNER", plan);
           // The planner prompt must explicitly forbid execution.
        Assert.Contains("execute-yaml-job", plan);
        Assert.Contains("FORBIDDEN", plan);
         }

        [Fact]
    public void Select_Execute_And_Autonomous_Uses_Executor_Prompt()
            {
            Assert.Contains("EXECUTOR", AgentSystemPrompt.Select(AgentMode.Execute));
            Assert.Contains("EXECUTOR", AgentSystemPrompt.Select(AgentMode.Autonomous));
             }

    /// <summary>
    /// The whole catalogue, not a name: a tool added with <see cref="WritesToTargetAttribute"/>
    /// is covered here without this file being edited, which is the point of reading the mark
    /// instead of keeping a list.
    /// </summary>
    [Fact]
    public void No_Tool_That_Writes_To_A_Target_Is_Offered_In_Plan_Mode()
        {
        var provider = new McpToolProvider(McpCatalogue.Tools());
        var writing = ToolModePolicy.WritingTools(typeof(DtPipeMcpTools));

        // A guard over an empty set is green for the wrong reason.
        Assert.NotEmpty(writing);

        var planTools = provider.GetToolDefinitions(AgentMode.Plan).Select(t => t.Name).ToList();
        foreach (var tool in writing)
            {
            Assert.DoesNotContain(tool, planTools);
            Assert.True(ToolModePolicy.IsBlockedInPlanMode(tool));
            }

        var allTools = provider.GetToolDefinitions(AgentMode.Execute).Select(t => t.Name).ToList();
        foreach (var tool in writing)
            Assert.Contains(tool, allTools);
        }

    /// <summary>
    /// 'dry-run' executes the real pipeline with the writer neutralised. Marking it would hide it
    /// from the planner, which its own role prompt tells it to call.
    /// </summary>
    [Fact]
    public void Dry_Run_Is_Not_A_Writing_Tool()
        {
        var writing = ToolModePolicy.WritingTools(typeof(DtPipeMcpTools));

        Assert.DoesNotContain("dry-run", writing);
        Assert.Contains(
            new McpToolProvider(McpCatalogue.Tools()).GetToolDefinitions(AgentMode.Plan),
            t => t.Name == "dry-run");
        }

}
