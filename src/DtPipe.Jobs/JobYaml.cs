using System.Text;
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

    private static readonly string[] YamlLineBreaks = ["\r\n", "\r", "\n", "\u0085", "\u2028", "\u2029"];

    private static readonly IReadOnlyDictionary<string, string> EmptyBranchComments = new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// Reads the raw leading <c>#</c> header comments of a job and any <c>#</c> comments placed directly
    /// under each top-level branch key before its first entry. Uses the YAML scanner so <c>#</c> inside
    /// scalars is never treated as a comment. When <paramref name="headerOnly"/> is true, stops as soon
    /// as the first top-level entry begins.
    /// </summary>
    public static (string? Header, IReadOnlyDictionary<string, string> BranchComments) ReadComments(string yaml, bool headerOnly = false)
    {
        var normalized = yaml.TrimStart('\uFEFF').Replace("\r\n", "\n").Replace('\r', '\n');
        var rawLines = normalized.Split('\n');
        var headerLines = new List<string>();
        var branchComments = headerOnly ? null : new Dictionary<string, string>(StringComparer.Ordinal);
        var pendingBranchLines = headerOnly ? null : new List<string>();

        static string CommentText(Comment c, string[] lines)
        {
            if (c.Start.Line == c.End.Line && c.Start.Line >= 1 && c.Start.Line <= lines.Length)
            {
                var trimmed = lines[(int)c.Start.Line - 1].TrimEnd('\r').TrimStart();
                if (trimmed.StartsWith('#'))
                {
                    var afterHash = trimmed[1..];
                    return afterHash.StartsWith(' ') ? afterHash[1..] : afterHash;
                }
            }
            return c.Value.StartsWith(' ') ? c.Value[1..] : c.Value;
        }

        var depth = 0;
        var rootMapping = false;
        var expectingRootKey = false;
        var rootKeyCount = 0;
        string? currentBranch = null;
        var awaitingBranchMapping = false;
        var inBranchLeading = false;

        try
        {
            var parser = new Parser(new Scanner(new StringReader(normalized), skipComments: false));
            while (parser.MoveNext())
            {
                switch (parser.Current)
                {
                    case DocumentEnd:
                    case StreamEnd:
                        return (headerLines.Count > 0 ? string.Join('\n', headerLines) : null, branchComments ?? EmptyBranchComments);

                    case Comment c:
                        if (c.IsInline) break;
                        var line = CommentText(c, rawLines);
                        if (rootKeyCount == 0 && (depth == 0 || (depth == 1 && rootMapping && expectingRootKey)))
                            headerLines.Add(line);
                        else if (!headerOnly && currentBranch is not null && (awaitingBranchMapping || inBranchLeading))
                            pendingBranchLines!.Add(line);
                        break;

                    case MappingStart:
                        depth++;
                        if (depth == 1)
                        {
                            rootMapping = true;
                            expectingRootKey = true;
                        }
                        else if (depth == 2 && awaitingBranchMapping)
                        {
                            awaitingBranchMapping = false;
                            inBranchLeading = true;
                        }
                        else
                        {
                            awaitingBranchMapping = false;
                            inBranchLeading = false;
                        }
                        break;

                    case MappingEnd:
                        if (!headerOnly && depth == 2 && currentBranch is not null)
                        {
                            if (pendingBranchLines!.Count > 0)
                                branchComments![currentBranch] = string.Join('\n', pendingBranchLines);
                            pendingBranchLines.Clear();
                            currentBranch = null;
                            inBranchLeading = false;
                            expectingRootKey = true;
                        }
                        depth--;
                        if (depth == 0)
                        {
                            rootMapping = false;
                            expectingRootKey = false;
                        }
                        break;

                    case SequenceStart:
                        depth++;
                        awaitingBranchMapping = false;
                        inBranchLeading = false;
                        if (headerOnly && depth == 1 && !rootMapping)
                            return (headerLines.Count > 0 ? string.Join('\n', headerLines) : null, EmptyBranchComments);
                        break;

                    case SequenceEnd:
                        depth--;
                        if (depth == 1 && rootMapping)
                        {
                            currentBranch = null;
                            expectingRootKey = true;
                        }
                        break;

                    case Scalar s:
                        if (depth == 1 && rootMapping && expectingRootKey)
                        {
                            currentBranch = s.Value;
                            rootKeyCount++;
                            if (headerOnly)
                                return (headerLines.Count > 0 ? string.Join('\n', headerLines) : null, EmptyBranchComments);
                            expectingRootKey = false;
                            awaitingBranchMapping = true;
                            pendingBranchLines!.Clear();
                        }
                        else if (depth == 1 && rootMapping && awaitingBranchMapping)
                        {
                            awaitingBranchMapping = false;
                            currentBranch = null;
                            expectingRootKey = true;
                            pendingBranchLines!.Clear();
                        }
                        else if (!headerOnly && depth == 2 && inBranchLeading)
                        {
                            if (pendingBranchLines!.Count > 0 && currentBranch is not null)
                                branchComments![currentBranch] = string.Join('\n', pendingBranchLines);
                            pendingBranchLines.Clear();
                            inBranchLeading = false;
                        }
                        break;
                }
            }
        }
        catch (YamlException)
        {
            if (headerLines.Count == 0)
            {
                foreach (var raw in rawLines)
                {
                    var trimmed = raw.TrimEnd('\r').TrimStart();
                    if (!trimmed.StartsWith('#')) break;
                    var afterHash = trimmed[1..];
                    headerLines.Add(afterHash.StartsWith(' ') ? afterHash[1..] : afterHash);
                }
            }
        }

        return (headerLines.Count > 0 ? string.Join('\n', headerLines) : null, branchComments ?? EmptyBranchComments);
    }

    /// <summary>Formats raw comment lines as <c>#</c> lines with optional indentation.</summary>
    public static string FormatComments(string? text, string indent = "")
    {
        if (string.IsNullOrWhiteSpace(text)) return "";
        var sb = new StringBuilder();
        foreach (var line in text.Split(YamlLineBreaks, StringSplitOptions.None))
            sb.Append(indent).Append(line.Length == 0 ? "#" : $"# {line}").Append('\n');
        return sb.ToString();
    }
}
