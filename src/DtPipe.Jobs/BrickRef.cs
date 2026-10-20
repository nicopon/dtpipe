using YamlDotNet.RepresentationModel;

namespace DtPipe.Jobs;

/// <summary>A branch of a job that reads a node's brick by reference: <c>input: brick:&lt;node&gt;/&lt;brick&gt;</c>.</summary>
public sealed record BrickReference(string Alias, string Node, string Brick)
{
    /// <summary>The brick's key in the coordinator's catalog.</summary>
    public string Key => $"{Node}/{Brick}";
}

/// <summary>
/// The reference a pipeline makes to a brick, which only the distributed family and the designer read: the engine never
/// sees <c>brick:</c>. A reference branch holds the reference alone, so the node composes the rest of the branch from its
/// own declarations; anything else that uses the scheme is a mistake <see cref="Misuse"/> names.
/// </summary>
public static class BrickRef
{
    public const string Scheme = "brick:";

    public static string Format(string node, string brick) => $"{Scheme}{node}/{brick}";

    /// <summary>The node and the brick of <paramref name="value"/>, or <c>false</c> when it is no reference.</summary>
    public static bool TryParse(string? value, out string node, out string brick)
    {
        (node, brick) = ("", "");
        if (value is null || !value.StartsWith(Scheme, StringComparison.Ordinal)) return false;
        var rest = value[Scheme.Length..];
        var slash = rest.IndexOf('/');
        if (slash <= 0 || slash == rest.Length - 1 || rest.IndexOf('/', slash + 1) >= 0) return false;
        (node, brick) = (rest[..slash], rest[(slash + 1)..]);
        return true;
    }

    /// <summary>The reference a branch makes: its <c>input</c> alone is a reference. Null for any other branch.</summary>
    public static BrickReference? Of(string alias, YamlMappingNode branch) =>
        branch.Children.Count == 1 && TryParse(JobYaml.Scalar(branch, "input"), out var node, out var brick)
            ? new BrickReference(alias, node, brick)
            : null;

    /// <summary>Every branch of <paramref name="job"/> that references a brick, by alias.</summary>
    public static IReadOnlyDictionary<string, BrickReference> In(YamlMappingNode job) =>
        JobYaml.Branches(job).Select(b => Of(b.Alias, b.Branch)).OfType<BrickReference>()
            .ToDictionary(r => r.Alias, StringComparer.Ordinal);

    /// <summary>
    /// What is wrong with the use of the scheme in a branch that is not a plain reference: a reference that is
    /// malformed or shares the branch with other keys, or one in a place that is not a source's <c>input</c>. Null when the
    /// branch uses it correctly or not at all.
    /// </summary>
    public static string? Misuse(string alias, YamlMappingNode branch)
    {
        foreach (var key in new[] { "input", "output" })
        {
            if (JobYaml.Scalar(branch, key) is not { } value || !value.StartsWith(Scheme, StringComparison.Ordinal)) continue;
            if (key == "output") return $"'{alias}' writes to a brick reference: only a source is referenced as brick:<node>/<brick>.";
            if (!TryParse(value, out _, out _)) return $"'{alias}' has a malformed brick reference '{value}': write brick:<node>/<brick>.";
            if (branch.Children.Count != 1) return $"'{alias}' references a brick and says more: the node composes the rest of the branch.";
        }
        return null;
    }
}
