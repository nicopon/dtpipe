using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Server;

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
/// <para>
/// What the catalog reads of each announced brick (its branch, canonical form, version and text) it keeps, and a brick
/// announced again is read anew. The node's name and whether it is online are read from the inventory at every call.
/// </para>
/// </summary>
public sealed class BrickCatalog(NodeInventory inventory)
{
    private sealed record Resolved(BrickView View, YamlMappingNode Branch, string Canonical, string Declared);

    /// <summary>What reading an announced brick gives, which depends on the brick alone; <see cref="Branch"/> is <c>null</c> for a brick whose text is no mapping.</summary>
    private sealed record Read(YamlMappingNode? Branch, string Canonical, string Version, string Yaml);

    // By the announced object: a brick announced again is another object, so it is read again.
    private readonly ConditionalWeakTable<BrickInfo, Read> _read = new();

    public IReadOnlyList<BrickView> List() => Resolve().Select(r => r.View).ToList();

    /// <summary>Every brick as <c>dtpipe studio</c> reads it: its branch as its node declares it, as JSON.</summary>
    public IReadOnlyList<CatalogBrick> Catalog() => Resolve().Select(r => new CatalogBrick(
        r.View.Key, r.View.Node, r.View.Id, KindOf(r.View.Kind), r.View.Title, r.View.Description, (JsonObject)JsonNode.Parse(r.Declared)!,
        r.View.Version, r.View.Schema?.Select(c => new CatalogColumn(c.Name, c.Type, c.IsNullable)).ToList(), r.View.SchemaError,
        r.View.Online)).ToList();

    public static string KindOf(BrickKind kind) => kind == BrickKind.Sink ? StepKinds.Sink : StepKinds.Source;

    public BrickView? Find(string key) => Resolve().FirstOrDefault(r => r.View.Key == key)?.View;

    public YamlMappingNode BranchOf(BrickView brick) => BranchOf(brick.Key);

    private YamlMappingNode BranchOf(string key) => JobYaml.CloneBranch(Resolve().First(r => r.View.Key == key).Branch);

    /// <summary>The brick a whole branch is, read the way every node host reads it (<see cref="BrickRules.FormOf"/>).</summary>
    public BrickView? Match(YamlMappingNode branch) => MatchAll(branch).FirstOrDefault();

    /// <summary>Every brick a whole branch is: one per node that offers it, in the inventory's order. A placement picks among them.</summary>
    public IReadOnlyList<BrickView> MatchAll(YamlMappingNode branch)
    {
        var form = BrickRules.FormOf(branch);
        return Resolve().Where(r => r.View.Kind == form.Kind && r.Canonical == form.Canonical).Select(r => r.View).ToList();
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
                options is not null && options.Children.TryGetValue(kv.Key, out var mine) && BrickRules.Canonical(mine) == BrickRules.Canonical(kv.Value));
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
                if (_read.GetValue(brick, ReadOf) is not { Branch: { } branch } read) continue;
                resolved.Add(new Resolved(
                    new BrickView($"{node.Name}/{brick.Id}", node.Name, brick.Id, brick.Kind, brick.Title, brick.Description, read.Yaml,
                        read.Version, brick.Schema, brick.SchemaError, node.Online),
                    branch, read.Canonical, brick.Branch));
            }
        }
        return resolved;
    }

    private static Read ReadOf(BrickInfo brick)
    {
        // The one reading of an announced brick, the node's and the replica chooser's too.
        if (FragmentJob.Declared(brick) is not { } branch) return new Read(null, "", "", "");
        var canonical = BrickRules.Declared(brick.Kind, branch).Canonical;
        var version = ContentHash.Of(canonical, 12);
        var yaml = JobYaml.Serialize(branch);
        return new Read(branch, canonical, version, yaml);
    }
}
