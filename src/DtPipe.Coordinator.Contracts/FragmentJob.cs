using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using DtPipe.Jobs;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Contracts;

/// <summary>
/// The one composition of a data node's job from the ordered units of a fragment and the bricks its owner
/// declared. The coordinator composes to plan, hashes, and sends the units; the node composes again from
/// its own declarations and compares: the same function on both sides is what makes the bytes, and so the
/// version, equal. The vocabulary is closed (<see cref="FragmentUnitKind"/>), the same a data node accepts
/// of a job in <see cref="BrickRules"/>.
/// </summary>
public static partial class FragmentJob
{
    /// <summary>The declared branch of the brick <paramref name="id"/> of that kind, or <c>null</c> when the node offers none.</summary>
    public delegate YamlMappingNode? BranchOfBrick(string id, BrickKind kind);

    /// <summary>
    /// The branch an announced brick declares, read the one way every side reads it: the node when it
    /// composes, the coordinator when it asks whether a replica could. <c>null</c> when the brick's text
    /// is no mapping. Two readings would let the coordinator call a replica eligible that then refuses
    /// every run.
    /// </summary>
    public static YamlMappingNode? Declared(BrickInfo brick)
    {
        try { return JobYaml.FromJson(JsonNode.Parse(brick.Branch)) as YamlMappingNode; }
        catch (Exception) { return null; }
    }

    /// <summary>The bricks a node announces as <see cref="BranchOfBrick"/>: a brick of the kind asked, by id.</summary>
    public static BranchOfBrick Over(IReadOnlyList<BrickInfo> bricks) =>
        (id, kind) => bricks.FirstOrDefault(b => b.Id == id && b.Kind == kind) is { } brick ? Declared(brick) : null;

    /// <summary>
    /// The job a data node composes for <paramref name="spec"/>'s units from the bricks it declares, or why it
    /// refuses: the units do not compose, or the bricks no longer give the version the plan was made with. The
    /// node decides it at <c>Launch</c>, and the coordinator asks the same of the bricks a replica announces
    /// before it picks one, so what one calls eligible the other runs.
    /// </summary>
    public static (string? Yaml, string? Reason) ComposeFor(FragmentSpec spec, BranchOfBrick branchOf)
    {
        if (spec.Units is not { } units) return (null, $"{spec.Fragment} names no brick to compose a job from.");
        var (composed, error) = Compose(units, branchOf);
        if (composed is null) return (null, error);
        if (VersionOf(composed) == spec.Version) return (composed, null);
        var bricks = string.Join(", ", units.Where(u => u.Brick is not null).Select(u => u.Brick).Distinct());
        return (null, $"a brick of {spec.Fragment} ({bricks}) changed since the plan was made: launch it again.");
    }

    [GeneratedRegex("^[A-Za-z][A-Za-z0-9_-]*$")]
    private static partial Regex Alias();

    /// <summary>The job <paramref name="units"/> compose to, or why they do not compose.</summary>
    public static (string? Yaml, string? Error) Compose(IReadOnlyList<FragmentUnit> units, BranchOfBrick branchOf)
    {
        var root = new YamlMappingNode();
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var unit in units)
        {
            if (!Alias().IsMatch(unit.Key)) return (null, $"'{unit.Key}' is not an alias: a letter first, then letters, digits, '_' or '-'.");
            if (!keys.Add(unit.Key)) return (null, $"'{unit.Key}' is the key of two pieces of the fragment.");
            YamlMappingNode branch;
            switch (unit.Kind)
            {
                case FragmentUnitKind.Inbound:
                    branch = new YamlMappingNode { { "input", BrickRules.ArrowEndpoint } };
                    break;
                case FragmentUnitKind.Source or FragmentUnitKind.SourceSend:
                    if (Declared(unit, BrickKind.Source, branchOf) is not { } source) return (null, NoBrick(unit, "source"));
                    branch = source;
                    if (unit.Kind == FragmentUnitKind.SourceSend)
                    {
                        if (JobYaml.Has(branch, "output")) return (null, $"'{unit.Key}': the brick '{unit.Brick}' already writes somewhere.");
                        JobYaml.Set(branch, "output", BrickRules.ArrowEndpoint);
                    }
                    break;
                case FragmentUnitKind.Sink:
                    if (Declared(unit, BrickKind.Sink, branchOf) is not { } sink) return (null, NoBrick(unit, "sink"));
                    if (unit.From is not { } feed || !Alias().IsMatch(feed)) return (null, $"'{unit.Key}' is a sink that reads no piece of the fragment.");
                    branch = sink;
                    if (JobYaml.Has(branch, "from")) return (null, $"'{unit.Key}': the brick '{unit.Brick}' already reads something.");
                    JobYaml.Set(branch, "from", feed);
                    break;
                case FragmentUnitKind.Relay:
                    if (unit.From is not { } relayed || !Alias().IsMatch(relayed)) return (null, $"'{unit.Key}' is a relay of no piece of the fragment.");
                    branch = new YamlMappingNode { { "from", relayed }, { "output", BrickRules.ArrowEndpoint } };
                    break;
                default:
                    return (null, $"'{unit.Key}' is a piece of an unknown kind.");
            }
            root.Add(unit.Key, branch);
        }
        foreach (var unit in units.Where(u => u.Kind is FragmentUnitKind.Relay or FragmentUnitKind.Sink))
        {
            if (unit.From == unit.Key || !keys.Contains(unit.From!)) return (null, $"'{unit.Key}' reads '{unit.From}', which is no other piece of the fragment.");
        }
        return (JobYaml.Serialize(root), null);
    }

    /// <summary>The version of a composed job: the hash admission compares, of the bytes a node writes.</summary>
    public static string VersionOf(string yaml) => ContentHash.Of(yaml);

    private static YamlMappingNode? Declared(FragmentUnit unit, BrickKind kind, BranchOfBrick branchOf) =>
        unit.Brick is { Length: > 0 } id ? branchOf(id, kind) is { } branch ? JobYaml.CloneBranch(branch) : null : null;

    private static string NoBrick(FragmentUnit unit, string kind) =>
        $"'{unit.Key}': this node offers no {kind} brick '{unit.Brick}'.";
}
