using System.Linq;

namespace DtPipe.Cli.Mcp;

/// <summary>
/// What the server tells a client about itself at the initialization handshake.
///
/// <para>
/// This is the only frame a model gets when dtpipe is driven as a plain MCP server. The agent
/// command pushes <see cref="DtPipe.Cli.Agent.AgentSystemPrompt"/> as the first message of every
/// session; nothing does that for an external host, so a model reaching dtpipe through one used to
/// receive fifteen tool descriptions and no relationship between them. Hosts typically hand these
/// instructions to the model as a system message, which is what makes this the right slot rather
/// than a prompt — a prompt has to be asked for, and only a person can ask.
/// </para>
///
/// <para>
/// It says what reflection cannot: which tool follows which, and which defaults bite. It must not
/// restate a tool description or the job shape — <c>help</c> owns the shape and every tool carries
/// its own description, both emitted from the source. The protocol asks for the same restraint.
/// </para>
/// </summary>
internal static class McpServerInstructions
{
    /// <summary>
    /// Tools named in <see cref="Text"/>. <c>McpServerInstructionsTests</c> checks each one against
    /// the live catalogue: a hand-written list of another component's names is stale the day one is
    /// renamed, and this is the cheap check that makes the drift impossible rather than unlikely.
    /// </summary>
    internal static readonly string[] ToolsNamed =
    [
        "help", "list-providers", "inspect", "preview-data",
        "validate-yaml-job", "dry-run", "execute-yaml-job",
    ];

    public const string Text = """
        dtpipe streams data between databases, files and object stores. A pipeline is always a YAML
        job, and every tool that takes a pipeline takes the same one — build it once and pass it on.

        Order that works:
          1. 'help' for the job shape and 'list-providers' for what exists. Call them before
             guessing a key or an adapter name; neither is restated anywhere else.
          2. 'inspect' for a real schema, 'preview-data' for real rows. Column names guessed from a
             file name are the commonest cause of a job that validates and then fails.
          3. 'validate-yaml-job' checks syntax, topology and option keys. It never opens the source,
             so it cannot know a column is missing — it says so in its own answer.
          4. 'dry-run' runs the real pipeline over a few source rows with the writer neutralised,
             and returns the rows as they leave each stage. A step that drops or multiplies rows is
             visible here and nowhere earlier.
          5. 'execute-yaml-job' writes. It is a dry run unless apply=true, and a real write also
             needs the approval gate to allow it; destructive SQL and network access are denied
             unless explicitly allowed. An answer that says nothing was written means nothing was
             written — re-read it before retrying.

        When a call comes back wrong, say what it told you and what you will change. An unknown YAML
        key is refused, never ignored, and the refusal names the key, its line and the legal ones.
        """;

    /// <summary>The text as sent, trimmed of the trailing newline a raw literal leaves.</summary>
    public static string Build() => Text.TrimEnd();

    /// <summary>True when every tool named in the text is present in <paramref name="catalogue"/>.</summary>
    internal static bool NamesOnlyExistingTools(System.Collections.Generic.IEnumerable<string> catalogue)
    {
        var known = catalogue.ToHashSet(System.StringComparer.OrdinalIgnoreCase);
        return ToolsNamed.All(known.Contains);
    }
}
