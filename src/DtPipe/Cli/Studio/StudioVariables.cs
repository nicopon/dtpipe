using DtPipe.Jobs;
using DtPipe.Cli.Security;

namespace DtPipe.Cli.Studio;

/// <summary>What a variable is launched with in the studio: a keyring alias, or a text the person typed.</summary>
public sealed record StudioOverride(string? Keyring = null, string? Value = null);

/// <summary>
/// A <c>${{…}}</c> of a job as the page shows it. <see cref="Kind"/> is <c>env</c>, <c>keyring</c>
/// or <c>cursor</c>; <see cref="Defined"/> says whether the environment holds the variable or the
/// keyring the alias, never what; <see cref="Ready"/> whether a sample would resolve it.
/// </summary>
public sealed record StudioVariable(string Expression, string Kind, string Name, bool Defined, StudioOverride? Override, bool Ready);

/// <summary>
/// The variables of a job, read with the run's own grammar (<see cref="VariableTokens"/>),
/// and what a sample launches them with. A variable's name is the contract: the job keeps
/// <c>${{NAME}}</c>, and the studio, like a node in production, decides what it resolves to. The
/// page sees names and aliases, never a value it did not type.
/// </summary>
public sealed class StudioVariables(StudioLocalFiles local, ISecretsManager secrets)
{
    private const string KeyringPrefix = "keyring://";
    private const string CursorPrefix = "cursor://";

    private IReadOnlyList<string>? _aliases;

    /// <summary>
    /// The keyring's aliases, never their values, kept for the session: opening the keyring can ask the person and wait.
    /// <paramref name="refresh"/> reads it again, as a name the list lacks asks for. None when the keyring cannot be
    /// read, which is not kept.
    /// </summary>
    public IReadOnlyList<string> Aliases(bool refresh = false)
    {
        if (!refresh && _aliases is { } kept) return kept;
        try { return _aliases = secrets.ListSecrets().Keys.Order(StringComparer.Ordinal).ToList(); }
        catch (Exception) { return []; }
    }

    public IReadOnlyList<StudioVariable> Of(string? job, string yaml)
    {
        var overrides = job is null ? new Dictionary<string, StudioOverride>() : local.ReadLaunch(job);
        // The keyring is opened only for a job that names it, once per session: on some systems opening it asks the person.
        // An alias the kept list lacks is looked for once more, so one added with `dtpipe secret set` is found without a restart.
        var refreshed = false;
        bool Holds(string alias)
        {
            if (Aliases().Contains(alias, StringComparer.OrdinalIgnoreCase)) return true;
            if (refreshed) return false;
            refreshed = true;
            return Aliases(refresh: true).Contains(alias, StringComparer.OrdinalIgnoreCase);
        }
        var variables = new List<StudioVariable>();
        foreach (var expression in VariableTokens.Expressions(yaml))
        {
            if (VariableTokens.Refusal(expression) is { } refusal) throw new StudioRefusal(422, refusal);
            if (expression.StartsWith(KeyringPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var alias = expression[KeyringPrefix.Length..];
                var known = Holds(alias);
                variables.Add(new StudioVariable(expression, "keyring", alias, known, null, known));
            }
            else if (expression.StartsWith(CursorPrefix, StringComparison.OrdinalIgnoreCase))
                variables.Add(new StudioVariable(expression, "cursor", expression[CursorPrefix.Length..], true, null, true));
            else
            {
                // The process's own environment: a value armed for a running action is never in it.
                var defined = Environment.GetEnvironmentVariable(expression) is not null;
                var chosen = overrides.GetValueOrDefault(expression);
                var ready = chosen switch
                {
                    { Keyring: { } alias } => Holds(alias),
                    { Value: not null } => true,
                    _ => defined,
                };
                variables.Add(new StudioVariable(expression, "env", expression, defined, chosen, ready));
            }
        }
        return variables;
    }

    /// <summary>
    /// What a sample of <paramref name="yaml"/> would resolve its variables with, as text to hash: a typed text and a process
    /// variable by value, a keyring alias by its name. Never opens the keyring, so a changed keyring value is not seen;
    /// empty when the job names no variable.
    /// </summary>
    public string Fingerprint(string? job, string yaml)
    {
        var overrides = job is null ? new Dictionary<string, StudioOverride>() : local.ReadLaunch(job);
        var material = new System.Text.StringBuilder();
        foreach (var expression in VariableTokens.Expressions(yaml).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal))
        {
            if (VariableTokens.Refusal(expression) is not null || expression.StartsWith(CursorPrefix, StringComparison.OrdinalIgnoreCase)) continue;
            string resolved;
            if (expression.StartsWith(KeyringPrefix, StringComparison.OrdinalIgnoreCase)) resolved = "keyring:" + expression[KeyringPrefix.Length..];
            else resolved = overrides.GetValueOrDefault(expression) switch
            {
                { Keyring: { } alias } => "keyring:" + alias,
                { Value: { } text } => "text:" + text,
                _ => "env:" + Environment.GetEnvironmentVariable(expression),
            };
            material.Append(expression).Append('=').Append(resolved).Append('\0');
        }
        return material.ToString();
    }

    /// <summary>
    /// The values a sample of <paramref name="yaml"/> is launched with, by variable name: each one's chosen
    /// value, for <see cref="LaunchValues.Arm"/>. Refuses, naming them, the variables a run would leave unresolved.
    /// </summary>
    public IReadOnlyDictionary<string, string> LaunchEnvironment(string? job, string yaml)
    {
        var variables = Of(job, yaml);
        var missing = variables.Where(v => !v.Ready)
            .Select(v => v.Kind == "keyring" ? $"the keyring alias '{v.Name}'" : $"'{v.Name}'").ToList();
        if (missing.Count > 0)
            throw new StudioRefusal(422,
                $"Nothing resolves {string.Join(", ", missing)}: choose a value in the Variables tab, set it in the environment, or add the alias with dtpipe secret set.");

        var environment = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var v in variables.Where(v => v.Override is not null))
        {
            var value = v.Override!.Keyring is { } alias ? secrets.GetSecret(alias) : v.Override.Value;
            environment[v.Name] = value ?? throw new StudioRefusal(422, $"The keyring alias '{v.Override.Keyring}' has no value.");
        }
        return environment;
    }
}
