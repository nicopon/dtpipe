using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using DtPipe.Cli.Agent;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Tool names written by hand, held against the live catalogue.
///
/// <para>
/// Three places name tools without asking the catalogue: the role prompts, the set whose results
/// are cached as facts, and the two names that move the plan badge. Each perishes silently — a
/// renamed tool leaves the model briefed on a name that no longer exists, stops a result being
/// cached, or freezes the badge — and none of the three symptoms points back here.
/// </para>
/// </summary>
public class AgentToolNameDriftTests
{
    /// <summary>
    /// Quoted, lowercase, hyphenated: the shape of a tool name in prose. A one-word quoted token
    /// such as 'help' cannot be told from an ordinary word, so it is not checked here — which
    /// leaves every multi-word name covered and says plainly which one is not.
    /// </summary>
    private static readonly Regex QuotedToolName = new(@"'([a-z][a-z0-9]*(?:-[a-z0-9]+)+)'", RegexOptions.Compiled);

    public static TheoryData<string, string> RolePrompts => new()
    {
        { nameof(AgentSystemPrompt.DefaultSystemPrompt), AgentSystemPrompt.DefaultSystemPrompt },
        { nameof(AgentSystemPrompt.PlannerSystemPrompt), AgentSystemPrompt.PlannerSystemPrompt },
        { nameof(AgentSystemPrompt.ExecutorSystemPrompt), AgentSystemPrompt.ExecutorSystemPrompt },
        { nameof(AgentSystemPrompt.ExternalServerFallbackPrompt), AgentSystemPrompt.ExternalServerFallbackPrompt },
    };

    [Theory]
    [MemberData(nameof(RolePrompts))]
    public void Every_Tool_A_Role_Prompt_Names_Exists(string which, string prompt)
    {
        var catalogue = McpCatalogue.Names();

        foreach (Match m in QuotedToolName.Matches(prompt))
        {
            var name = m.Groups[1].Value;
            Assert.True(
                catalogue.Contains(name, StringComparer.OrdinalIgnoreCase),
                $"{which} tells the model to call '{name}', which the catalogue does not carry.");
        }
    }

    /// <summary>
    /// The prompts are handed to a model verbatim, so an XML escape in one reaches it as the escape.
    /// Three of them shipped as "Discovery &amp;amp; Guidelines".
    /// </summary>
    [Theory]
    [MemberData(nameof(RolePrompts))]
    public void No_Role_Prompt_Carries_An_Xml_Entity(string which, string prompt)
    {
        foreach (var entity in new[] { "&amp;", "&lt;", "&gt;", "&quot;", "&apos;" })
            Assert.False(prompt.Contains(entity, StringComparison.Ordinal), $"{which} carries {entity}.");
    }

    [Fact]
    public void Every_Tool_Whose_Result_Is_Cached_As_A_Fact_Exists()
    {
        var catalogue = McpCatalogue.Names();

        Assert.NotEmpty(AgentExecutor.FactProducingTools);
        foreach (var tool in AgentExecutor.FactProducingTools)
            Assert.Contains(tool, catalogue, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Every_Tool_That_Moves_The_Plan_Exists()
    {
        var catalogue = McpCatalogue.Names();

        Assert.Contains(PlanProgress.ValidateTool, catalogue, StringComparer.OrdinalIgnoreCase);
        Assert.Contains(PlanProgress.ExecuteTool, catalogue, StringComparer.OrdinalIgnoreCase);
    }
}
