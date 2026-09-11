using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F6 — a pipeline reaches a tool as text, in the call itself.
///
/// <para>
/// That is what lets a model drive dtpipe from a host with no file tool of its own: there is
/// nothing to write to disk first. A parameter taking a path would move the pipeline into a
/// filesystem the caller may not share with the server, and the failure would surface as a job
/// that cannot be found rather than as a missing capability.
/// </para>
///
/// <para>
/// Read off the reflected catalogue, so a tool added tomorrow is covered without editing this file.
/// </para>
/// </summary>
public class McpInlinePipelineTests
{
    /// <summary>Spellings a path parameter would take; compared on letters, in one case.</summary>
    private static readonly string[] PathSpellings =
    [
        "path", "file", "filepath", "jobfile", "jobpath",
        "yamlfile", "yamlpath", "configfile", "configpath", "configurationfile",
    ];

    /// <summary>The one spelling the inline pipeline parameter is allowed to have.</summary>
    private const string Inline = "yamlContent";

    [Fact]
    public void No_Tool_Asks_For_A_Pipeline_As_A_File_Path()
    {
        var tools = McpToolReflector.BuildToolDefinitions(typeof(DtPipeMcpTools));
        Assert.NotEmpty(tools);

        foreach (var tool in tools)
        {
            foreach (var (name, _) in Parameters(tool))
            {
                Assert.False(
                    PathSpellings.Contains(Normalise(name)),
                    $"Tool '{tool.Name}' takes '{name}': a pipeline must be passed inline, not as a path.");
            }
        }
    }

    /// <summary>
    /// Catches the rename rather than the path: a parameter that says YAML and is called something
    /// else is a second way to submit a pipeline, and the single-path property is gone whether or
    /// not it happens to be a file name.
    /// </summary>
    [Fact]
    public void A_Parameter_That_Carries_Yaml_Is_Always_Called_yamlContent()
    {
        var tools = McpToolReflector.BuildToolDefinitions(typeof(DtPipeMcpTools));
        var carriers = 0;

        foreach (var tool in tools)
        {
            foreach (var (name, schema) in Parameters(tool))
            {
                var describesYaml = schema.TryGetProperty("description", out var d)
                    && (d.GetString() ?? string.Empty).Contains("YAML", StringComparison.OrdinalIgnoreCase);

                if (!describesYaml) continue;

                carriers++;
                Assert.Equal(Inline, name);
                Assert.Equal("string", schema.GetProperty("type").GetString());
                Assert.Contains(Inline, Required(tool));
            }
        }

        // A guard that matched nothing would stay green through a rename of every one of them.
        Assert.True(carriers >= 2, "No tool was found taking a pipeline; the guard would be vacuous.");
    }

    private static IEnumerable<(string Name, JsonElement Schema)> Parameters(ToolDefinition tool)
    {
        if (!tool.ParametersSchema.TryGetProperty("properties", out var props)) yield break;
        foreach (var p in props.EnumerateObject())
            yield return (p.Name, p.Value);
    }

    private static IReadOnlyList<string> Required(ToolDefinition tool)
        => tool.ParametersSchema.TryGetProperty("required", out var req)
            ? req.EnumerateArray().Select(e => e.GetString() ?? string.Empty).ToList()
            : [];

    private static string Normalise(string name) =>
        name.Replace("-", string.Empty).Replace("_", string.Empty).ToLowerInvariant();
}
