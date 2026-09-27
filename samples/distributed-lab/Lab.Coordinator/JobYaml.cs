using System.Text.RegularExpressions;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Lab.Coordinator;

/// <summary>
/// Reads and writes dtpipe job files as a plain YAML tree: one top-level key per branch alias. The
/// lab moves whole branches between fragments and adds <c>arrow:-</c> endpoints; it never
/// interprets a branch's options, which stay dtpipe's business.
/// </summary>
public static partial class JobYaml
{
    public static YamlMappingNode Parse(string yaml)
    {
        var stream = new YamlStream();
        using (var reader = new StringReader(yaml)) stream.Load(reader);
        if (stream.Documents.Count == 0 || stream.Documents[0].RootNode is not YamlMappingNode root)
            throw new FormatException("A job file is a mapping of branch aliases.");
        foreach (var (key, value) in root.Children)
        {
            if (value is not YamlMappingNode)
                throw new FormatException($"Branch '{key}' is not a mapping.");
        }
        return root;
    }

    public static string Serialize(YamlMappingNode root)
    {
        var stream = new YamlStream(new YamlDocument(root));
        using var writer = new StringWriter { NewLine = "\n" };
        stream.Save(writer, assignAnchors: false);
        var text = writer.ToString();
        // YamlStream closes every document with an explicit end marker; a job file has one document.
        return text.EndsWith("...\n", StringComparison.Ordinal) ? text[..^4] : text;
    }

    public static YamlMappingNode CloneBranch(YamlMappingNode branch)
    {
        var wrapper = new YamlMappingNode { { "b", branch } };
        return (YamlMappingNode)Parse(Serialize(wrapper))["b"];
    }

    public static IEnumerable<(string Alias, YamlMappingNode Branch)> Branches(YamlMappingNode root) =>
        root.Children.Select(kv => (((YamlScalarNode)kv.Key).Value!, (YamlMappingNode)kv.Value));

    public static string? Scalar(YamlMappingNode branch, string key) =>
        branch.Children.TryGetValue(new YamlScalarNode(key), out var node) && node is YamlScalarNode s ? s.Value : null;

    public static YamlMappingNode? Mapping(YamlMappingNode branch, string key) =>
        branch.Children.TryGetValue(new YamlScalarNode(key), out var node) ? node as YamlMappingNode : null;

    public static bool Has(YamlMappingNode branch, string key) => branch.Children.ContainsKey(new YamlScalarNode(key));

    public static void Set(YamlMappingNode branch, string key, string value) =>
        branch.Children[new YamlScalarNode(key)] = new YamlScalarNode(value);

    /// <summary>
    /// The aliases a branch reads through <c>from</c> or <c>ref</c>. dtpipe writes <c>from</c> as a
    /// comma-separated scalar and <c>ref</c> as a sequence; both shapes are accepted for each.
    /// </summary>
    public static IReadOnlyList<string> Aliases(YamlMappingNode branch, string key)
    {
        if (!branch.Children.TryGetValue(new YamlScalarNode(key), out var node)) return [];
        IEnumerable<string> raw = node switch
        {
            YamlScalarNode s => (s.Value ?? "").Split(','),
            YamlSequenceNode seq => seq.Children.OfType<YamlScalarNode>().Select(s => s.Value ?? ""),
            _ => [],
        };
        return raw.Select(a => a.Trim()).Where(a => a.Length > 0).ToList();
    }

    public static IReadOnlyList<YamlMappingNode> Transformers(YamlMappingNode branch) =>
        branch.Children.TryGetValue(new YamlScalarNode("transformers"), out var node) && node is YamlSequenceNode seq
            ? seq.Children.OfType<YamlMappingNode>().ToList()
            : [];

    /// <summary>Environment variables a job text resolves through <c>${{NAME}}</c>.</summary>
    public static IReadOnlyList<string> Variables(string text) =>
        VariablePattern().Matches(text).Select(m => m.Groups[1].Value).Distinct(StringComparer.Ordinal).ToList();

    [GeneratedRegex(@"\$\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")]
    private static partial Regex VariablePattern();
}
