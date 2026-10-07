using DtPipe.Cli.Expressions;
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
/// The variables of a job, read with the run's own grammar (<see cref="CompositeStringContentResolver.Expressions"/>),
/// and what a sample launches them with. A variable's name is the contract: the job keeps
/// <c>${{NAME}}</c>, and the studio, like a node in production, decides what it resolves to. The
/// page sees names and aliases, never a value it did not type.
/// </summary>
public sealed class StudioVariables(StudioLocalFiles local, ISecretsManager secrets)
{
    private const string KeyringPrefix = "keyring://";
    private const string CursorPrefix = "cursor://";

    /// <summary>The keyring's aliases, never their values; none when the keyring cannot be read.</summary>
    public IReadOnlyList<string> Aliases()
    {
        try { return secrets.ListSecrets().Keys.Order(StringComparer.Ordinal).ToList(); }
        catch (Exception) { return []; }
    }

    public IReadOnlyList<StudioVariable> Of(string? job, string yaml)
    {
        var overrides = job is null ? new Dictionary<string, StudioOverride>() : local.ReadLaunch(job);
        // The keyring is opened only for a job that names it: on some systems opening it asks the person.
        var keyring = new Lazy<HashSet<string>>(() => Aliases().ToHashSet(StringComparer.OrdinalIgnoreCase));
        var variables = new List<StudioVariable>();
        foreach (var expression in CompositeStringContentResolver.Expressions(yaml))
        {
            if (expression.StartsWith(KeyringPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var alias = expression[KeyringPrefix.Length..];
                var known = keyring.Value.Contains(alias);
                variables.Add(new StudioVariable(expression, "keyring", alias, known, null, known));
            }
            else if (expression.StartsWith(CursorPrefix, StringComparison.OrdinalIgnoreCase))
                variables.Add(new StudioVariable(expression, "cursor", expression[CursorPrefix.Length..], true, null, true));
            else
            {
                var defined = Environment.GetEnvironmentVariable(expression) is not null;
                var chosen = overrides.GetValueOrDefault(expression);
                var ready = chosen switch
                {
                    { Keyring: { } alias } => keyring.Value.Contains(alias),
                    { Value: not null } => true,
                    _ => defined,
                };
                variables.Add(new StudioVariable(expression, "env", expression, defined, chosen, ready));
            }
        }
        return variables;
    }

    /// <summary>
    /// The environment a sample of <paramref name="yaml"/> runs with: each variable's chosen value.
    /// Refuses, naming them, the variables a run would leave unresolved.
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
