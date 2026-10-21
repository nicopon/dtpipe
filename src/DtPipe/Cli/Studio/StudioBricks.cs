using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The coordinator's bricks as the studio's designer sees them. <see cref="Find"/> and
/// <see cref="BranchOf"/> read the catalog the studio read last; <see cref="Match"/> reads back what the
/// coordinator answered for each reference branch of the job being opened, found again by the branch's YAML.
/// That YAML is only a key to the answer: deciding that a reference names a brick stays the coordinator's.
/// </summary>
public sealed class StudioBricks : IBrickLookup
{
    private readonly Dictionary<string, CatalogBrick> _bricks;
    private readonly Dictionary<string, Recognition> _answers = new(StringComparer.Ordinal);

    /// <param name="catalog">The catalog read last, or null for none.</param>
    /// <param name="job">The job the answers were given for.</param>
    /// <param name="answers">The coordinator's answer for each top-level branch of <paramref name="job"/>, by alias.</param>
    public StudioBricks(BrickCatalogView? catalog, string? job = null, IReadOnlyDictionary<string, Recognition>? answers = null)
    {
        _bricks = (catalog?.Bricks ?? []).ToDictionary(b => b.Key, StringComparer.Ordinal);
        if (job is null || answers is null) return;
        foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(job)))
            if (answers.TryGetValue(alias, out var answer)) _answers[Key(branch)] = answer;
    }

    public CatalogBrick? Brick(string key) => _bricks.GetValueOrDefault(key);

    public DesignBrick? Find(string key) => _bricks.TryGetValue(key, out var b) ? new DesignBrick(b.Key, b.Kind, b.Node, b.Version, b.Title, b.Online) : null;

    public YamlMappingNode BranchOf(DesignBrick brick) => _bricks.TryGetValue(brick.Key, out var b)
        ? (YamlMappingNode)JobYaml.FromJson(b.Branch)
        : throw new InvalidOperationException($"No brick '{brick.Key}' is in the catalog read last.");

    public DesignBrick? Match(YamlMappingNode branch) => Answer(branch)?.Match is { } key ? Find(key) : null;

    private Recognition? Answer(YamlMappingNode branch) => _answers.GetValueOrDefault(Key(branch));

    private static string Key(YamlMappingNode branch) => JobYaml.Serialize(branch);
}
