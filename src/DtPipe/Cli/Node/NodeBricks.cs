using System.Text.RegularExpressions;
using DtPipe.Cli.Expressions;
using DtPipe.Cli.Security;
using DtPipe.Core.Security;
using DtPipe.Jobs;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Cli.Node;

/// <summary>A branch of the command line read as a brick: <see cref="Yaml"/> is the branch a node composes its job from.</summary>
internal sealed record DeclaredBrick(string Id, bool IsSource, string Yaml);

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
            bricks.Add(new DeclaredBrick(alias, isSource, JobYaml.Serialize(branch)));
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
    /// guesses offline. The branch is judged by <see cref="JobSecrets"/>, the one walk of the secret judge over a job as written.
    /// </summary>
    private static string? SecretRefusal(string alias, YamlMappingNode branch) =>
        JobSecrets.InBranch(branch).FirstOrDefault() is { } path
            ? $"Branch '{alias}' holds a credential as a value ({path}): a brick's version is a hash of its text. Use ${{{{NAME}}}} or keyring://alias."
            : null;

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
