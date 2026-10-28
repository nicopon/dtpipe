using System.Text.RegularExpressions;
using DtPipe.Cli.Expressions;
using DtPipe.Cli.Security;
using DtPipe.Core.Security;
using DtPipe.Jobs;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Cli.Node;

/// <summary>A branch of the command line read as a brick: <see cref="BranchJson"/> is the branch a node composes its job from.</summary>
internal sealed record DeclaredBrick(string Id, bool IsSource, string BranchJson);

/// <summary>
/// The bricks a <c>dtpipe node</c> command line declares: each branch is one, named by its alias. The branches are the job file
/// the command line stands for (<c>--export-job</c>'s path), so <c>-i … --alias a</c> and the same branch in a <c>--job</c> file
/// give the same text, and so the same version.
/// </summary>
internal static class NodeBricks
{
    /// <summary>The bricks of <paramref name="jobYaml"/>, or why one cannot be.</summary>
    public static (IReadOnlyList<DeclaredBrick> Bricks, string? Refusal) Read(string jobYaml)
    {
        var bricks = new List<DeclaredBrick>();
        foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(jobYaml)))
        {
            if (Shape(alias, branch) is { } shape) return ([], shape);
            if (SecretRefusal(alias, branch) is { } secret) return ([], secret);
            var isSource = JobYaml.Has(branch, JobKeys.Input);
            bricks.Add(new DeclaredBrick(alias, isSource, JobYaml.ToJson(branch)!.ToJsonString()));
        }
        return bricks.Count == 0 ? ([], "a node offers at least one brick: declare a branch (-i/-o with --alias) or give --job.") : (bricks, null);
    }

    // A brick reads or writes, never both: the node's job is composed from its own declarations, and anything between a
    // read and a write belongs on a runner. The alias is the brick's id, so a branch without one has none.
    private static string? Shape(string alias, YamlMappingNode branch)
    {
        var reads = JobYaml.Has(branch, JobKeys.Input);
        var writes = JobYaml.Has(branch, JobKeys.Output);
        if (JobYaml.Has(branch, JobKeys.From))
            return $"Branch '{alias}' reads another branch (from): a brick reads a source or writes a sink, and everything between belongs on a runner.";
        if (reads == writes)
            return reads
                ? $"Branch '{alias}' reads and writes: a brick is either a source (-i) or a sink (-o). Each brick is a branch of its own, and a --job file holds several."
                : $"Branch '{alias}' neither reads nor writes: a brick is either a source (-i) or a sink (-o).";
        return null;
    }

    /// <summary>
    /// A brick's version is the hash of its text, so a credential written into it would let anyone who sees the version test
    /// guesses offline. Every scalar is judged with the vocabulary <c>dtpipe split</c> uses on a job, as written: a
    /// <c>${{NAME}}</c> or <c>keyring://</c> reference passes, a value does not. Best-effort by construction, in both directions: a
    /// credential under a key the vocabulary does not know passes, and a query that compares a column named like one
    /// (<c>token_count = 5</c>) is refused.
    /// </summary>
    private static string? SecretRefusal(string alias, YamlMappingNode branch)
    {
        foreach (var (path, value) in Scalars(branch, ""))
        {
            var key = path[(path.LastIndexOf('.') + 1)..];
            if (ConnectionStringSanitizer.CarriesLiteralSecret(value) || ConnectionStringSanitizer.CarriesLiteralSecret($"{key}={value}"))
                return $"Branch '{alias}' holds a credential as a value ({path}): a brick's version is a hash of its text. Use ${{{{NAME}}}} or keyring://alias.";
        }
        return null;
    }

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

    /// <summary>A reference to a secret rather than a value: <c>keyring://alias</c>, <c>${{NAME}}</c> or <c>@file</c>.</summary>
    public static bool IsSecretReference(string text) =>
        text.StartsWith("keyring://", StringComparison.OrdinalIgnoreCase)
        || text.StartsWith('@')
        || Regex.IsMatch(text, @"^\$\{\{[^}]+\}\}$");

    /// <summary>
    /// Why a flag that takes a secret refuses a value, with the two lines that give it as a reference. <paramref name="what"/> is how
    /// the text calls the secret ("secret", "key"); an argument is listed by any process of the host and kept by a shell.
    /// </summary>
    public static string NotAReference(string flag, string what, string variable) =>
        $"{flag} takes a reference, never the {what}: an argument is listed by any process of the host. Read it into a variable "
        + $"(read -rs {variable} && export {variable}) and pass {flag} '${{{{{variable}}}}}'; keyring://alias and @file work too.";

    /// <summary>
    /// The secret <paramref name="reference"/> names (<c>${{NAME}}</c>, <c>keyring://alias</c> or <c>@file</c>), read once, or why not;
    /// <paramref name="what"/> is how the text calls it ("client secret", "key"). A reference the resolver leaves as it was (a variable
    /// that is not set, an alias the keyring lacks) is refused by name: left alone it would pass for the secret and fail at the
    /// provider like a wrong one. No message carries a file's path, which may be a secret pasted after the <c>@</c>.
    /// </summary>
    public static async Task<(string? Secret, string? Refusal)> ReadSecretAsync(string reference, string what, ISecretsManager secrets, CancellationToken ct)
    {
        string? secret;
        try { secret = (await CompositeStringContentResolver.Standard(secrets).ResolveAsync(reference, ct))?.Trim(); }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (null, reference.StartsWith('@') && ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException
                ? $"The {what} names a file (@…) that cannot be read: {ex.GetType().Name}."
                : $"The {what} could not be read: {ConnectionStringSanitizer.Sanitize(ex.Message)}");
        }
        if (string.IsNullOrEmpty(secret)) return (null, $"The {what} resolves to nothing.");

        var unresolved = VariableTokens.Expressions(secret).FirstOrDefault()
            ?? (secret.StartsWith("keyring://", StringComparison.OrdinalIgnoreCase) ? secret : null);
        if (unresolved is null) return (secret, null);
        var shown = ConnectionStringSanitizer.Sanitize(unresolved);
        return (null, VariableTokens.IsReference(unresolved)
            ? $"The {what} names {shown}, which the keyring does not hold: store it first (dtpipe secret set), or name a variable or a file."
            : $"The {what} names the variable {shown}, which is not set here: export it where this command runs (read -rs {shown} && export {shown}).");
    }
}
