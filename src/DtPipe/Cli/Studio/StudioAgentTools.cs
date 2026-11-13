using System.Text.Json;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The tools the studio's agent is offered: dtpipe's, as <see cref="McpToolProvider"/> offers them (no tool that writes to a
/// target, F1), and the studio's own gestures on the folder. <c>save-job</c> and <c>run-job</c> write: each waits for the
/// person's answer to a card, which nothing of the command line, the environment or the model skips. A tool that runs the
/// engine waits for the studio's slot, so a page's sample and the agent's never run together.
/// </summary>
internal sealed class StudioAgentTools : IAgentToolProvider
{
    private readonly IAgentToolProvider _inner;
    private readonly StudioServer _server;
    private readonly List<ToolDefinition> _offered;

    public StudioAgentTools(IAgentToolProvider inner, StudioServer server)
    {
        _inner = inner;
        _server = server;
        _offered = [.. inner.GetToolDefinitions(), .. Definitions];
    }

    /// <summary>The studio's own tools, as the model reads them.</summary>
    internal static IReadOnlyList<ToolDefinition> Definitions { get; } =
    [
        Tool("list-jobs", "List the jobs of the folder the studio has open: their paths, relative to it.", """{"type":"object","properties":{}}"""),
        Tool("read-job", "Read the text of a job of the folder.",
            """{"type":"object","properties":{"path":{"type":"string","description":"The job's path in the folder."}},"required":["path"]}"""),
        Tool("save-job",
            "Write a job into the folder, as the studio composes it from your text. The person confirms it first; the call returns once they answered.",
            """{"type":"object","properties":{"path":{"type":"string","description":"The job's path in the folder, ending in .yaml or .yml."},"yamlContent":{"type":"string","description":"The job's YAML."}},"required":["path","yamlContent"]}"""),
        Tool("run-job",
            "Run a job saved in the folder for real: it writes its targets. The person confirms what it writes first; the call returns once the run ended, with its exit code (130: the person stopped it).",
            """{"type":"object","properties":{"path":{"type":"string","description":"The saved job's path in the folder."}},"required":["path"]}"""),
    ];

    private static ToolDefinition Tool(string name, string description, string schema) =>
        new(name, description, JsonDocument.Parse(schema).RootElement.Clone());

    public List<ToolDefinition> GetToolDefinitions() => _offered;

    public bool RunsPipeline(string toolName) =>
        _inner.RunsPipeline(toolName) || McpToolReflector.Normalise(toolName) is "savejob" or "runjob";

    public async Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
    {
        switch (McpToolReflector.Normalise(toolName))
        {
            case "listjobs":
                return ToolResult.FromJson(_server.AgentJobs());
            case "readjob":
                return ToolResult.FromJson(await _server.AgentReadJobAsync(Arg(args, "path"), ct));
            case "savejob":
                return ToolResult.FromJson(await _server.AgentSaveJobAsync(Arg(args, "path"), Arg(args, "yamlContent") ?? Arg(args, "yaml"), ct));
            case "runjob":
                return ToolResult.FromJson(await _server.AgentRunJobAsync(Arg(args, "path"), ct));
        }
        if (!_inner.RunsPipeline(toolName)) return await _inner.InvokeToolAsync(toolName, args, ct);
        using (await _server.TakeSlotAsync("The agent's dry-run", ct))
            return await _inner.InvokeToolAsync(toolName, args, ct);
    }

    private static string? Arg(JsonElement args, string name) =>
        args.ValueKind == JsonValueKind.Object && args.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
}
