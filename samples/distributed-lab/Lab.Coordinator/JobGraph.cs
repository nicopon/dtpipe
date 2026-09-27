using YamlDotNet.RepresentationModel;

namespace DtPipe.Lab.Coordinator;

public sealed record StageView(string Type, string Summary);

/// <summary>
/// What the page draws for one branch. <see cref="Options"/> flattens <c>provider-options</c> into
/// <c>component.key = value</c> lines, as written, without interpreting them.
/// </summary>
public sealed record BranchView(
    string Alias, string? Input, string? Output, IReadOnlyList<string> From, IReadOnlyList<string> Ref,
    string? Processor, IReadOnlyList<StageView> Transformers, IReadOnlyList<string> Options,
    IReadOnlyList<string> Variables)
{
    /// <summary>Only a branch that reads a source can be cut inside: that is what <c>dtpipe split</c> accepts.</summary>
    public bool Cuttable => Input is not null && From.Count == 0;
}

public static class JobGraph
{
    // provider-options keys that name a processor rather than a reader or writer.
    private static readonly string[] Processors = ["sql", "merge"];

    public static IReadOnlyList<BranchView> Describe(YamlMappingNode root) =>
        JobYaml.Branches(root).Select(b => Describe(b.Alias, b.Branch)).ToList();

    public static BranchView Describe(string alias, YamlMappingNode branch)
    {
        var providerOptions = JobYaml.Mapping(branch, "provider-options");
        var processor = providerOptions?.Children.Keys.OfType<YamlScalarNode>()
            .Select(k => k.Value!).FirstOrDefault(k => Processors.Contains(k));

        var options = new List<string>();
        if (providerOptions is not null)
        {
            foreach (var (component, value) in providerOptions.Children)
            {
                if (value is YamlMappingNode settings && settings.Children.Count > 0)
                    options.AddRange(settings.Children.Select(kv => $"{component}.{kv.Key} = {Flatten(kv.Value)}"));
                else
                    options.Add($"{component}");
            }
        }

        return new BranchView(
            alias,
            JobYaml.Scalar(branch, "input"),
            JobYaml.Scalar(branch, "output"),
            JobYaml.Aliases(branch, "from"),
            JobYaml.Aliases(branch, "ref"),
            processor,
            JobYaml.Transformers(branch).Select(DescribeStage).ToList(),
            options,
            JobYaml.Variables(JobYaml.Serialize(new YamlMappingNode { { alias, branch } })));
    }

    private static StageView DescribeStage(YamlMappingNode transformer)
    {
        var type = JobYaml.Scalar(transformer, "type") ?? "?";
        var parts = transformer.Children
            .Where(kv => kv.Key is YamlScalarNode { Value: not "type" })
            .Select(kv => kv.Value switch
            {
                YamlMappingNode m => string.Join(", ", m.Children.Select(c => $"{c.Key}: {Flatten(c.Value)}")),
                _ => $"{kv.Key}: {Flatten(kv.Value)}",
            });
        return new StageView(type, string.Join("; ", parts));
    }

    private static string Flatten(YamlNode node) => node switch
    {
        YamlScalarNode s => s.Value ?? "",
        YamlSequenceNode seq => string.Join(", ", seq.Children.Select(Flatten)),
        YamlMappingNode m => "{" + string.Join(", ", m.Children.Select(c => $"{c.Key}: {Flatten(c.Value)}")) + "}",
        _ => "",
    };
}
