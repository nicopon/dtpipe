using DtPipe.Core.Security;
using DtPipe.Jobs;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Cli.Security;

/// <summary>
/// Where a job holds a credential as a value, judged as written: every scalar of each branch, with its key, through
/// <see cref="ConnectionStringSanitizer.CarriesLiteralSecret"/>. A <c>${{NAME}}</c> or <c>keyring://</c> reference passes, a value
/// does not. Best-effort by construction, in both directions: a credential under a key the vocabulary does not know passes,
/// and a query that compares a column named like one (<c>token_count = 5</c>) is reported. It names where, never the value.
/// </summary>
public static class JobSecrets
{
    /// <summary>Each <c>branch.path</c> that carries a literal secret. A text that does not parse is judged line by line.</summary>
    public static IReadOnlyList<string> Literals(string yaml)
    {
        YamlMappingNode job;
        try { job = JobYaml.Parse(yaml); }
        catch (Exception)
        {
            return yaml.Split('\n').Select((line, i) => (Line: line.Trim(), Number: i + 1))
                .Where(l => l.Line.Length > 0 && !l.Line.StartsWith('#') && Judged(l.Line.Replace(": ", "=")))
                .Select(l => $"line {l.Number}").ToList();
        }
        var found = new List<string>();
        foreach (var (alias, branch) in JobYaml.Branches(job))
            found.AddRange(InBranch(branch).Select(path => $"{alias}.{path}"));
        return found;
    }

    /// <summary>The paths of one branch that carry a literal secret, in the order the branch is written.</summary>
    public static IEnumerable<string> InBranch(YamlMappingNode branch)
    {
        foreach (var (path, value) in Scalars(branch, ""))
        {
            var key = path[(path.LastIndexOf('.') + 1)..];
            if (Judged(value) || Judged($"{key}={value}")) yield return path;
        }
    }

    private static bool Judged(string text) => ConnectionStringSanitizer.CarriesLiteralSecret(text);

    private static IEnumerable<(string Path, string Value)> Scalars(YamlNode node, string path)
    {
        switch (node)
        {
            case YamlScalarNode { Value: { } text }:
                yield return (path, text);
                break;
            case YamlMappingNode map:
                foreach (var (k, v) in map.Children)
                    foreach (var scalar in Scalars(v, path.Length == 0 ? k.ToString() : $"{path}.{k}")) yield return scalar;
                break;
            case YamlSequenceNode seq:
                foreach (var item in seq.Children)
                    foreach (var scalar in Scalars(item, path)) yield return scalar;
                break;
        }
    }
}
