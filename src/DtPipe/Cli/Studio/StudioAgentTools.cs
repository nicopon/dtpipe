using System.Text.Json;
using DtPipe.Cli.Agent;
using DtPipe.Cli.Mcp;
using DtPipe.Jobs;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The tools the studio's agent is offered: dtpipe's, as <see cref="McpToolProvider"/> offers them (no tool that writes to a
/// target, F1), and the studio's own gestures on the folder. <c>save-job</c> writes a job, never a target: it waits for the
/// person's answer to a card, which nothing of the command line, the environment or the model skips. No tool runs a job for
/// real: that is the page's <c>Write targets…</c>. A tool that runs the engine waits for the studio's slot, so a page's sample
/// and the agent's never run together.
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
    ];

    private static ToolDefinition Tool(string name, string description, string schema) =>
        new(name, description, JsonDocument.Parse(schema).RootElement.Clone());

    public List<ToolDefinition> GetToolDefinitions() => _offered;

    public bool RunsPipeline(string toolName) =>
        _inner.RunsPipeline(toolName) || McpToolReflector.Normalise(toolName) == "savejob";

    public async Task<ToolResult> InvokeToolAsync(string toolName, JsonElement args, CancellationToken ct)
    {
        switch (McpToolReflector.Normalise(toolName))
        {
            case "listjobs":
                return ToolResult.FromJson(_server.AgentJobs());
            case "readjob":
                return ToolResult.FromJson(await _server.AgentReadJobAsync(Arg(args, "path"), ct));
            case "savejob":
                {
                    var yaml = Arg(args, "yamlContent") ?? Arg(args, "yaml");
                    if (await RefusalOfInvalidJobAsync(yaml, ct) is { } refusal) return ToolResult.FromJson(refusal);
                    return ToolResult.FromJson(await _server.AgentSaveJobAsync(Arg(args, "path"), yaml, ct));
                }
        }
        if (!_inner.RunsPipeline(toolName)) return await _inner.InvokeToolAsync(toolName, args, ct);
        using (await _server.TakeSlotAsync("The agent's dry-run", ct))
            return await _inner.InvokeToolAsync(toolName, args, ct);
    }

    /// <summary>
    /// The refusal of a job <c>validate-yaml-job</c> refuses, before the person is shown a card for it: composing cards keeps a
    /// branch no card describes as written, an option key the engine does not know included, so the card alone would offer to
    /// write a file no run can read. The check is the tool's own, never a second validator. A job in which a branch still
    /// references a brick is the engine's to refuse at its run, as the page's own save lets it through. Null: nothing to refuse.
    /// </summary>
    private async Task<string?> RefusalOfInvalidJobAsync(string? yaml, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(yaml) || !_inner.GetToolDefinitions().Any(t => McpToolReflector.Normalise(t.Name) == "validateyamljob"))
            return null;
        try { if (BrickRef.In(JobYaml.Parse(yaml)).Count > 0) return null; }
        catch (Exception ex) when (ex is FormatException or YamlDotNet.Core.YamlException) { /* the check below reports it */ }

        var verdict = await _inner.InvokeToolAsync("validate-yaml-job", JsonSerializer.SerializeToElement(new { yamlContent = yaml }), ct);
        using var doc = JsonDocument.Parse(verdict.Content);
        var root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("success", out var ok) || ok.ValueKind != JsonValueKind.False)
            return null;

        var reasons = root.TryGetProperty("errors", out var errors) && errors.ValueKind == JsonValueKind.Array
            ? string.Join(" ", errors.EnumerateArray().Select(e => e.GetString()))
            : root.TryGetProperty("error", out var one) ? one.GetString() : "";
        return JsonSerializer.Serialize(new
        {
            error = "Nothing was shown to the person and nothing was written: 'validate-yaml-job' refuses this job. " + reasons
                + " Change the job, check it with 'validate-yaml-job', then call 'save-job' again.",
        });
    }

    private static string? Arg(JsonElement args, string name) =>
        args.ValueKind == JsonValueKind.Object && args.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
}
