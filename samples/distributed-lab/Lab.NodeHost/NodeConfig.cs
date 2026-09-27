using System.Text.Json;

namespace DtPipe.Lab.NodeHost;

public sealed record DatasetConfig(string Variable, string Engine, string File, string Description);

/// <summary>A node's identity and the databases it hosts, read from <c>nodes/&lt;name&gt;.json</c>.</summary>
public sealed record NodeConfig(string Name, string Group, string Description, IReadOnlyList<DatasetConfig> Datasets)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static NodeConfig Load(string path) =>
        JsonSerializer.Deserialize<NodeConfig>(File.ReadAllText(path), Json)
            ?? throw new InvalidOperationException($"{path}: empty node configuration.");
}

public sealed record NodeHostOptions(string ConfigPath, string StateDir, string CoordinatorUrl, string DtPipeExecutable)
{
    public static NodeHostOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i + 1 < args.Length; i += 2)
            values[args[i]] = args[i + 1];

        string Required(string key) => values.TryGetValue(key, out var v)
            ? v
            : throw new ArgumentException($"Missing {key}. Usage: --config <node.json> --state <dir> --coordinator <url> --dtpipe <path>");

        return new NodeHostOptions(
            Path.GetFullPath(Required("--config")),
            Path.GetFullPath(Required("--state")),
            Required("--coordinator").TrimEnd('/'),
            Path.GetFullPath(Required("--dtpipe")));
    }
}
