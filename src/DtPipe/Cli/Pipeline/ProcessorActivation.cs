using DtPipe.Core.Abstractions;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// How a caller selects a stream processor, read off its factory: the <c>provider-options</c> key of
/// a YAML job (the factory's <see cref="IStreamTransformerFactory.ComponentName"/>, the convention
/// <see cref="IStreamTransformerFactory.IsApplicable(DtPipe.Core.Models.JobDefinition)"/> documents)
/// and the flags of the command line. The validator's refusal and the MCP's help both word it here, so
/// a processor added or renamed reaches both and neither hand-writes a name.
/// </summary>
internal static class ProcessorActivation
{
    /// <summary>The key under <c>provider-options</c> that selects the processor in a job file.</summary>
    public static string YamlKey(IStreamTransformerFactory processor) => processor.ComponentName;

    /// <summary>The command-line flags that select it, as typed (<c>--sql &lt;value&gt;</c>, <c>--merge</c>).</summary>
    public static IReadOnlyList<string> CliForms(IStreamTransformerFactory processor)
        => processor.CliTriggerFlags
            .Select(t => t.IsBoolean ? t.Flag : $"{t.Flag} <value>")
            .ToList();

    /// <summary>One processor's way in, in both surfaces: <c>sql: 'provider-options: sql' in YAML, --sql &lt;value&gt; on the command line</c>.</summary>
    public static string Describe(IStreamTransformerFactory processor)
    {
        var yaml = $"'provider-options: {YamlKey(processor)}' in YAML";
        var cli = CliForms(processor);
        return cli.Count == 0
            ? $"{processor.ComponentName}: {yaml}"
            : $"{processor.ComponentName}: {yaml}, {string.Join(", ", cli)} on the command line";
    }

    /// <summary>What a processor asks of the branches around it, in words: <c>exactly 1</c>, <c>at least 2</c>, <c>none</c>, <c>any number</c>.</summary>
    public static string Arity(int min, int max) => (min, max) switch
    {
        (0, 0) => "none",
        (0, < 0) => "any number",
        (_, < 0) => $"at least {min}",
        _ when min == max => $"exactly {min}",
        _ => $"{min} to {max}",
    };
}
