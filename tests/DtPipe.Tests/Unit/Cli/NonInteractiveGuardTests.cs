using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli;
using DtPipe.Cli.Agent;
using ModelContextProtocol.Server;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A tool call the agent's LLM made must never block on a keypress, even though
/// dtpipe agent's in-process MCP tools share the same real console/stdin as its own TUI. See the
/// dry-run interactive-viewer bug this guards against — a manual run where the agent's "dry-run"
/// tool call launched the keyboard-navigable viewer and waited for Enter.
/// </summary>
public class NonInteractiveGuardTests
{
    [Fact]
    public void Not_Suppressed_By_Default()
    {
        Assert.False(NonInteractiveGuard.IsSuppressed);
    }

    [Fact]
    public void Suppress_Sets_And_Restores_On_Dispose()
    {
        Assert.False(NonInteractiveGuard.IsSuppressed);
        using (NonInteractiveGuard.Suppress())
        {
            Assert.True(NonInteractiveGuard.IsSuppressed);
        }
        Assert.False(NonInteractiveGuard.IsSuppressed);
    }

    [Fact]
    public void Nested_Suppress_Restores_The_Outer_Scope_Not_The_Global_Default()
    {
        using (NonInteractiveGuard.Suppress())
        {
            using (NonInteractiveGuard.Suppress())
            {
                Assert.True(NonInteractiveGuard.IsSuppressed);
            }
            // Still inside the outer scope.
            Assert.True(NonInteractiveGuard.IsSuppressed);
        }
        Assert.False(NonInteractiveGuard.IsSuppressed);
    }

    [Fact]
    public async Task Flows_Across_Await_Continuations_Including_On_The_Thread_Pool()
    {
        using var _ = NonInteractiveGuard.Suppress();
        await Task.Run(() => Assert.True(NonInteractiveGuard.IsSuppressed));
        await Task.Yield();
        Assert.True(NonInteractiveGuard.IsSuppressed);
    }

    private sealed class GuardCheckingTools
    {
        [McpServerTool(Name = "check-guard")]
        [System.ComponentModel.Description("Reports whether NonInteractiveGuard is suppressed at invocation time.")]
        public string CheckGuard() => JsonSerializer.Serialize(new { suppressed = NonInteractiveGuard.IsSuppressed });
    }

    [Fact]
    public async Task McpToolProvider_Suppresses_For_The_Duration_Of_A_Tool_Call_Only()
    {
        var provider = new McpToolProvider(new GuardCheckingTools());
        Assert.False(NonInteractiveGuard.IsSuppressed);

        var result = await provider.InvokeToolAsync("check-guard", JsonDocument.Parse("{}").RootElement, CancellationToken.None);

        using var doc = JsonDocument.Parse(result.Content);
        Assert.True(doc.RootElement.GetProperty("suppressed").GetBoolean());
        Assert.False(NonInteractiveGuard.IsSuppressed);
    }
}
