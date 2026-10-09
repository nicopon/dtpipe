using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Cli.Studio;

/// <summary>A card that is a coordinator's brick, by the branch of the composed job that runs it (<see cref="StepInfo.Branch"/>).</summary>
public sealed record SampledBrick(string Branch, string Key);

/// <summary>
/// A job sampled on this machine although some of its branches are a node's bricks: each source
/// brick's branch becomes <c>input: arrow:-</c>, bound to the rows its node sends; each sink brick's
/// becomes <c>output: arrow:-</c>, bound to a throwaway file, so its target is never reached. The
/// bindings go through the run's own <c>--bind-input</c>/<c>--bind-output</c>.
/// </summary>
public sealed record BrickSampling(string Yaml, IReadOnlyList<(string Alias, CatalogBrick Brick)> Sources, IReadOnlyList<string> Sinks)
{
    public const string ArrowLink = "arrow:-";

    /// <summary>
    /// The job with each brick's branch replaced, or the job as it is with none. A brick the catalog does
    /// not hold, or a branch the job does not have, is refused by name.
    /// </summary>
    public static BrickSampling Of(string yaml, IReadOnlyList<SampledBrick>? bricks, BrickCatalogView? catalog)
    {
        if (bricks is not { Count: > 0 }) return new BrickSampling(yaml, [], []);
        var root = JobYaml.Parse(yaml);
        var sources = new List<(string, CatalogBrick)>();
        var sinks = new List<string>();
        foreach (var (alias, key) in bricks)
        {
            var brick = catalog?.Bricks.FirstOrDefault(b => b.Key == key)
                ?? throw new StudioRefusal(409, $"'{alias}' is the brick {key}, which no catalog read from the coordinator holds: sign in to it.");
            if (!root.Children.TryGetValue(new YamlScalarNode(alias), out var node) || node is not YamlMappingNode branch)
                throw new StudioRefusal(400, $"The job has no branch '{alias}'.");
            if (brick.Kind == StepKinds.Sink)
            {
                var sink = new YamlMappingNode();
                if (JobYaml.Scalar(branch, "from") is { } from) sink.Add("from", from);
                sink.Add("output", ArrowLink);
                root.Children[new YamlScalarNode(alias)] = sink;
                sinks.Add(alias);
            }
            else
            {
                root.Children[new YamlScalarNode(alias)] = new YamlMappingNode { { "input", ArrowLink } };
                sources.Add((alias, brick));
            }
        }
        return new BrickSampling(JobYaml.Serialize(root), sources, sinks);
    }
}
