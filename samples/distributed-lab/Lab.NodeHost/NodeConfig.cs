using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Lab.Contracts;

namespace DtPipe.Lab.NodeHost;

public sealed record DatasetConfig(string Variable, string Engine, string File, string Description);

/// <summary>A brick as the node's file declares it: <see cref="Branch"/> is a piece of a dtpipe branch.</summary>
public sealed record BrickConfig(string Id, BrickKind Kind, string Title, string? Description, JsonElement Branch);

/// <summary>
/// A node's name, credentials, the databases it hosts and the bricks it offers, read from
/// <c>nodes/&lt;name&gt;.json</c>. Its group is not here: the coordinator's IDP decides it.
/// </summary>
public sealed record NodeConfig(
    string Name, string Description, IReadOnlyList<DatasetConfig>? Datasets, NodeRole Role = NodeRole.Data,
    IReadOnlyList<BrickConfig>? Bricks = null, string? ClientId = null, string? Secret = null)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    public static NodeConfig Load(string path) =>
        JsonSerializer.Deserialize<NodeConfig>(File.ReadAllText(path), Json)
            ?? throw new InvalidOperationException($"{path}: empty node configuration.");
}

/// <summary>
/// <summary>
/// <see cref="Sandbox"/> lets a data node run a fragment whose branches are not its bricks. It is set
/// by <see cref="SandboxVariable"/> in the host process's own environment, never by anything the
/// coordinator sends; only <c>1</c> or <c>true</c> turns it on, so any other value stays strict.
/// </summary>
public sealed record NodeHostOptions(string ConfigPath, string StateDir, string CoordinatorUrl, string DtPipeExecutable, bool Sandbox = false)
{
    public const string SandboxVariable = "LAB_NODE_SANDBOX";

    public static NodeHostOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i + 1 < args.Length; i += 2)
            values[args[i]] = args[i + 1];

        string Required(string key) => values.TryGetValue(key, out var v)
            ? v
            : throw new ArgumentException($"Missing {key}. Usage: --config <node.json> --state <dir> --coordinator <url> --dtpipe <path>");

        var sandbox = Environment.GetEnvironmentVariable(SandboxVariable) is { } flag
            && (flag == "1" || flag.Equals("true", StringComparison.OrdinalIgnoreCase));

        return new NodeHostOptions(
            Path.GetFullPath(Required("--config")),
            Path.GetFullPath(Required("--state")),
            Required("--coordinator").TrimEnd('/'),
            Path.GetFullPath(Required("--dtpipe")),
            sandbox);
    }
}
