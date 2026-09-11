using System;
using System.Linq;
using System.Reflection;
using DtPipe.Cli.Mcp;
using ModelContextProtocol.Server;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The instructions are the only frame a plain MCP client gets, and they are hand-written — the one
/// place in this server that names tools without reflecting on them. That is the shape CLAUDE.md
/// calls stale the day something is renamed, so the names are checked against the live catalogue
/// here rather than trusted.
/// </summary>
public class McpServerInstructionsTests
{
    private static string[] Catalogue() =>
        typeof(DtPipeMcpTools)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .Where(n => !string.IsNullOrEmpty(n))
            .Select(n => n!)
            .ToArray();

    [Fact]
    public void Every_Tool_Named_In_The_Instructions_Exists()
    {
        var catalogue = Catalogue();
        Assert.NotEmpty(catalogue);

        var missing = McpServerInstructions.ToolsNamed
            .Where(n => !catalogue.Contains(n, StringComparer.OrdinalIgnoreCase))
            .ToArray();

        Assert.True(missing.Length == 0,
            $"The server instructions name tools the catalogue no longer has: {string.Join(", ", missing)}");
    }

    /// <summary>
    /// The declared list is what the check above runs on, so a name added to the text and not to
    /// the list would be unchecked — which is the drift, one level up.
    /// </summary>
    [Fact]
    public void The_Declared_List_Covers_Every_Name_Quoted_In_The_Text()
    {
        var quoted = System.Text.RegularExpressions.Regex
            .Matches(McpServerInstructions.Text, @"'([a-z][a-z0-9-]+)'")
            .Select(m => m.Groups[1].Value)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var undeclared = quoted
            .Where(q => !McpServerInstructions.ToolsNamed.Contains(q, StringComparer.OrdinalIgnoreCase))
            .ToArray();

        Assert.True(undeclared.Length == 0,
            $"Quoted in the instructions but absent from ToolsNamed: {string.Join(", ", undeclared)}");
    }

    /// <summary>
    /// The protocol asks these instructions not to duplicate what tool descriptions already carry,
    /// and 'help' owns the job shape. A YAML skeleton pasted here is the second copy that rots.
    /// </summary>
    [Fact]
    public void The_Instructions_Do_Not_Restate_The_Job_Shape()
    {
        Assert.DoesNotContain("provider-options:", McpServerInstructions.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("transformers:", McpServerInstructions.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void The_Instructions_Are_Sent_Trimmed_And_Not_Empty()
    {
        var built = McpServerInstructions.Build();

        Assert.False(string.IsNullOrWhiteSpace(built));
        Assert.Equal(built.TrimEnd(), built);
    }
}
