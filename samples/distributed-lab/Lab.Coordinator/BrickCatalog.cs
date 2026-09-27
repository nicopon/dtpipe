using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using DtPipe.Lab.Contracts;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Lab.Coordinator;

/// <summary>
/// One brick as the page sees it. <see cref="Key"/> is <c>node/id</c>; <see cref="Version"/> hashes
/// the brick's canonical branch, so it changes exactly when what dtpipe would run changes.
/// </summary>
public sealed record BrickView(
    string Key, string Node, string Id, BrickKind Kind, string Title, string Description, string Yaml, string Version,
    IReadOnlyList<BrickColumn>? Schema, string? SchemaError, bool Online);

/// <summary>
/// Every brick the nodes announce. A branch of a job <b>is</b> a brick when its content equals the
/// brick's, never because of a name: a job stays a plain dtpipe job, and a brick its owner changed
/// stops matching the pipelines written against its old form.
/// </summary>
public sealed class BrickCatalog(NodeInventory inventory)
{
    private sealed record Resolved(BrickView View, YamlMappingNode Branch, string Canonical);

    private static readonly YamlScalarNode FromKey = new("from");

    public IReadOnlyList<BrickView> List() => Resolve().Select(r => r.View).ToList();

    public BrickView? Find(string key) => Resolve().FirstOrDefault(r => r.View.Key == key)?.View;

    public YamlMappingNode BranchOf(BrickView brick) =>
        JobYaml.CloneBranch(Resolve().First(r => r.View.Key == brick.Key).Branch);

    /// <summary>The brick a whole branch is: a source read as declared, or a sink fed through <c>from</c>.</summary>
    public BrickView? Match(YamlMappingNode branch)
    {
        var isSink = JobYaml.Has(branch, "from");
        var withoutFrom = new YamlMappingNode(branch.Children.Where(kv => !kv.Key.Equals(FromKey)));
        var canonical = JobYaml.Canonical(withoutFrom);
        return Resolve().FirstOrDefault(r => r.View.Kind == (isSink ? BrickKind.Sink : BrickKind.Source) && r.Canonical == canonical)?.View;
    }

    /// <summary>
    /// The brick a branch starts with (a source: same <c>input</c>, and each of the brick's
    /// provider-options components present unchanged) or ends with (a sink: same <c>output</c>). The
    /// designer's import uses it to take a branch that does several things apart into one step each.
    /// </summary>
    public BrickView? Contained(YamlMappingNode branch, BrickKind kind)
    {
        var endpoint = kind == BrickKind.Source ? "input" : "output";
        var value = JobYaml.Scalar(branch, endpoint);
        if (value is null) return null;
        var options = JobYaml.Mapping(branch, "provider-options");
        foreach (var r in Resolve().Where(r => r.View.Kind == kind))
        {
            if (JobYaml.Scalar(r.Branch, endpoint) != value) continue;
            var brickOptions = JobYaml.Mapping(r.Branch, "provider-options");
            var contained = brickOptions is null || brickOptions.Children.All(kv =>
                options is not null && options.Children.TryGetValue(kv.Key, out var mine) && JobYaml.Canonical(mine) == JobYaml.Canonical(kv.Value));
            var onlyKnownKeys = r.Branch.Children.Keys.All(k => k is YamlScalarNode { Value: "input" or "output" or "provider-options" });
            if (contained && onlyKnownKeys) return r.View;
        }
        return null;
    }

    private List<Resolved> Resolve()
    {
        var resolved = new List<Resolved>();
        foreach (var node in inventory.Snapshot())
        {
            foreach (var brick in node.Bricks)
            {
                YamlMappingNode branch;
                try { branch = (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(brick.Branch)); }
                catch (Exception) { continue; }
                var canonical = JobYaml.Canonical(branch);
                var version = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)))[..12];
                var yaml = JobYaml.Serialize(branch);
                resolved.Add(new Resolved(
                    new BrickView($"{node.Name}/{brick.Id}", node.Name, brick.Id, brick.Kind, brick.Title, brick.Description, yaml,
                        version, brick.Schema, brick.SchemaError, node.Online),
                    branch, canonical));
            }
        }
        return resolved;
    }
}
