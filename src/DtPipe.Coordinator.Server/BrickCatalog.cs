using System.Text.Json.Nodes;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// One brick as the page sees it. <see cref="Key"/> is <c>node/id</c>; <see cref="Yaml"/> is the reference a job makes to it;
/// <see cref="Version"/> is the keyed hash its node took of the brick's canonical branch, so it changes exactly when what dtpipe
/// would run changes.
/// </summary>
public sealed record BrickView(
    string Key, string Node, string Id, BrickKind Kind, string Title, string Description, string Yaml, string Version, bool Online);

/// <summary>
/// Every brick the nodes announce, by its reference: a job names a brick by node and id (<c>brick:&lt;node&gt;/&lt;id&gt;</c>), and a brick its
/// owner changed has another version, which stops the pipelines checked against the old one. A node announces no text, so there is
/// none to match. The node's name and whether it is online are read from the inventory at every call.
/// </summary>
public sealed class BrickCatalog(NodeInventory inventory)
{
    private sealed record Resolved(BrickView View, YamlMappingNode Branch);

    public IReadOnlyList<BrickView> List() => Resolve().Select(r => r.View).ToList();

    /// <summary>Every brick as <c>dtpipe studio</c> reads it: the reference a job makes to it, as JSON, and never its text.</summary>
    public IReadOnlyList<CatalogBrick> Catalog() => Resolve().Select(r => new CatalogBrick(
        r.View.Key, r.View.Node, r.View.Id, KindOf(r.View.Kind), r.View.Title, r.View.Description, (JsonObject)JsonNode.Parse(JobYaml.ToJson(r.Branch)!.ToJsonString())!,
        r.View.Version, r.View.Online)).ToList();

    public static string KindOf(BrickKind kind) => kind == BrickKind.Sink ? StepKinds.Sink : StepKinds.Source;

    public BrickView? Find(string key) => Resolve().FirstOrDefault(r => r.View.Key == key)?.View;

    /// <summary>The reference a job makes to <paramref name="brick"/>: all the coordinator holds of the branch a node composes for it.</summary>
    public YamlMappingNode BranchOf(BrickView brick) => JobYaml.CloneBranch(Resolve().First(r => r.View.Key == brick.Key).Branch);

    private List<Resolved> Resolve()
    {
        var resolved = new List<Resolved>();
        foreach (var node in inventory.Snapshot())
        {
            foreach (var brick in node.Bricks)
            {
                if (string.IsNullOrWhiteSpace(brick.Version)) continue;
                // A brick is read as the reference a job makes to it, which names the node as it is called now.
                var branch = ReferenceTo(node.Name, brick);
                resolved.Add(new Resolved(
                    new BrickView($"{node.Name}/{brick.Id}", node.Name, brick.Id, brick.Kind, brick.Title, brick.Description,
                        JobYaml.Serialize(branch), brick.Version, node.Online),
                    branch));
            }
        }
        return resolved;
    }

    // A source is read by an input reference and a sink written by an output one.
    private static YamlMappingNode ReferenceTo(string node, BrickInfo brick) =>
        new() { { brick.Kind == BrickKind.Sink ? "output" : "input", BrickRef.Format(node, brick.Id) } };
}
