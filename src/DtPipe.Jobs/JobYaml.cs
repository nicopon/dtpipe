using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Jobs;

/// <summary>
/// Reads and writes dtpipe job files as a plain YAML tree: one top-level key per branch alias. Its
/// readers move whole branches, or take one apart at its endpoints; none interprets a branch's
/// options, which stay dtpipe's business.
/// </summary>
public static class JobYaml
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

    /// <summary>
    /// One branch written as YAML text, a JSON object included (YAML reads JSON): the one reader of a brick its owner declares.
    /// The tree keeps each scalar's text, the way dtpipe reads a job, and every key is a scalar: a brick's form is read by key.
    /// Its presentation is a job file's, whatever the text's: block collections and <see cref="Text"/>'s scalars, so a branch
    /// written as JSON is not written back on one line.
    /// </summary>
    public static YamlMappingNode ParseBranch(string text)
    {
        var stream = new YamlStream();
        using (var reader = new StringReader(text)) stream.Load(reader);
        if (stream.Documents.Count != 1 || stream.Documents[0].RootNode is not YamlMappingNode branch)
            throw new FormatException("A branch is one mapping.");
        if (HasComplexKey(branch)) throw new FormatException("Every key of a branch is a scalar.");
        AsJobFile(branch);
        return branch;
    }

    private static bool HasComplexKey(YamlNode node) => node switch
    {
        YamlMappingNode m => m.Children.Any(kv => kv.Key is not YamlScalarNode || HasComplexKey(kv.Value)),
        YamlSequenceNode s => s.Children.Any(HasComplexKey),
        _ => false,
    };

    private static void AsJobFile(YamlNode node)
    {
        switch (node)
        {
            case YamlMappingNode m:
                m.Style = MappingStyle.Block;
                foreach (var (key, value) in m.Children)
                {
                    AsJobFile(key);
                    AsJobFile(value);
                }
                break;
            case YamlSequenceNode s:
                s.Style = SequenceStyle.Block;
                foreach (var item in s.Children) AsJobFile(item);
                break;
            case YamlScalarNode { Value: { } value } scalar:
                scalar.Style = Text(value).Style;
                break;
        }
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

    public static YamlScalarNode Text(string value) =>
        new(value) { Style = value.Contains('\n') ? ScalarStyle.Literal : ScalarStyle.Any };

    /// <summary>Environment variables a job text resolves through <c>${{NAME}}</c>: the names of <see cref="VariableTokens"/>.</summary>
    public static IReadOnlyList<string> Variables(string text) =>
        VariableTokens.Expressions(text).Where(VariableTokens.IsName).ToList();

    /// <summary>The variables one branch reads, as <see cref="Variables(string)"/> reads them from its text.</summary>
    public static IReadOnlyList<string> Variables(string alias, YamlMappingNode branch) =>
        Variables(Serialize(new YamlMappingNode { { alias, branch } }));
}
