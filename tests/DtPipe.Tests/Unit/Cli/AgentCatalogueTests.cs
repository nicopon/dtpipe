using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using DtPipe.Tests.Helpers;
using ModelContextProtocol.Server;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F1 — the model is never offered a tool that writes to a target, whatever the tool is called: the
/// catalogue is read off the mark, and the role prompt says the same.
/// </summary>
public class AgentCatalogueTests
{
    /// <summary>
    /// A tools class that mirrors the real MCP surface: a planning tool and the execution tool
    /// '<c>execute-yaml-job</c>' that must never be offered.
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
    public void The_Catalogue_Excludes_Every_Writing_Tool_But_Keeps_Planning_Tools()
    {
        var provider = new McpToolProvider(new PlanAndExecuteTools());

        var tools = provider.GetToolDefinitions();
        Assert.DoesNotContain(tools, t => t.Name == "execute-yaml-job");
        Assert.DoesNotContain(tools, t => t.Name == "push-to-warehouse");
        Assert.Contains(tools, t => t.Name == "validate-yaml-job");
    }

    [Fact]
    public void The_Execution_Tool_Is_Marked_As_Writing_And_A_Planning_Tool_Is_Not()
    {
        var writing = ToolModePolicy.WritingTools(typeof(DtPipeMcpTools));

        Assert.Contains("execute-yaml-job", writing);
        Assert.DoesNotContain("validate-yaml-job", writing);
    }

    /// <summary>The answer follows the person's language, a fixed line: no browser setting reaches the prompt.</summary>
    [Fact]
    public void The_Prompt_Asks_For_The_Language_Of_The_Persons_Last_Message()
    {
        Assert.Contains("language of the person's last message", AgentSystemPrompt.Studio);
    }

    /// <summary>A write waits for the person in the tool call: a prompt saying the model cannot execute would be false.</summary>
    [Fact]
    public void The_Prompt_Says_A_Save_Waits_For_The_Person_And_Sends_A_Real_Run_To_The_Page()
    {
        var prompt = AgentSystemPrompt.Studio;

        Assert.Contains("'save-job'", prompt);
        Assert.DoesNotContain("run-job", prompt);
        Assert.Contains("Write targets", prompt);
        Assert.Contains("confirm", prompt);
        Assert.DoesNotContain("FORBIDDEN", prompt, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("execute-yaml-job", prompt);
    }

    /// <summary>
    /// The whole catalogue, not a name: a tool added with <see cref="WritesToTargetAttribute"/>
    /// is covered here without this file being edited, which is the point of reading the mark
    /// instead of keeping a list.
    /// </summary>
    [Fact]
    public void No_Tool_That_Writes_To_A_Target_Is_Offered()
    {
        var provider = new McpToolProvider(McpCatalogue.Tools());
        var writing = ToolModePolicy.WritingTools(typeof(DtPipeMcpTools));

        // A guard over an empty set is green for the wrong reason.
        Assert.NotEmpty(writing);

        var offered = provider.GetToolDefinitions().Select(t => t.Name).ToList();
        foreach (var tool in writing)
            Assert.DoesNotContain(tool, offered);

        // Everything the server carries that does not write is offered, and nothing else is.
        var expected = McpCatalogue.Names().Where(n => !writing.Contains(n)).OrderBy(n => n, StringComparer.Ordinal);
        Assert.Equal(expected, offered.OrderBy(n => n, StringComparer.Ordinal));
    }

    /// <summary>
    /// F5 over the whole catalogue: the agent serialises exactly the tools marked as running a
    /// pipeline, read off the mark. A write is a run, so every writing tool is among them.
    /// </summary>
    [Fact]
    public void The_Agent_Serialises_Exactly_The_Tools_That_Run_A_Pipeline()
    {
        var provider = new McpToolProvider(McpCatalogue.Tools());
        var running = ToolModePolicy.RunningTools(typeof(DtPipeMcpTools));
        var writing = ToolModePolicy.WritingTools(typeof(DtPipeMcpTools));

        Assert.Contains("dry-run", running);
        Assert.Subset(running.ToHashSet(), writing.ToHashSet());

        foreach (var tool in provider.GetToolDefinitions())
            Assert.Equal(running.Contains(tool.Name), provider.RunsPipeline(tool.Name));
        foreach (var tool in running)
            Assert.True(provider.RunsPipeline(tool));
        foreach (var tool in running.Where(t => !writing.Contains(t)))
            Assert.Contains(provider.GetToolDefinitions(), t => t.Name == tool);
    }

    /// <summary>
    /// 'dry-run' executes the real pipeline with the writer neutralised. Marking it would hide it
    /// from the model, which its own role prompt tells it to call.
    /// </summary>
    [Fact]
    public void Dry_Run_Is_Not_A_Writing_Tool()
    {
        var writing = ToolModePolicy.WritingTools(typeof(DtPipeMcpTools));

        Assert.DoesNotContain("dry-run", writing);
        Assert.Contains(
            new McpToolProvider(McpCatalogue.Tools()).GetToolDefinitions(),
            t => t.Name == "dry-run");
    }
}
