using System.Text;
using DtPipe.Jobs;
using DtPipe.Cli.Incremental;
using DtPipe.Cli.Security;
using DtPipe.Core.Expressions;

namespace DtPipe.Cli.Expressions;

/// <summary>
/// Resolves string values through a sequential composite pipeline:
/// 1. @file → load file content
/// 2. Full string exact match through interpolators (e.g. keyring://alias)
/// 3. Inline substitution through interpolators (e.g. ${{ENV_VAR}}, ${{cursor://path}})
/// </summary>
public sealed class CompositeStringContentResolver : IStringContentResolver
{
    private readonly IEnumerable<IStringInterpolator> _interpolators;

    /// <summary>
    /// The resolver every run uses, the one place the chain's order is fixed: launch values, keyring, cursor,
    /// then the environment, which answers any name and so comes last. Without a secrets manager the
    /// chain has no keyring.
    /// </summary>
    public static CompositeStringContentResolver Standard(ISecretsManager? secrets)
    {
        var chain = new List<IStringInterpolator> { new LaunchValueInterpolator() };
        if (secrets != null) chain.Add(new KeyringInterpolator(secrets));
        chain.Add(new CursorInterpolator());
        chain.Add(new EnvVarInterpolator());
        return new CompositeStringContentResolver(chain);
    }

    public CompositeStringContentResolver(IEnumerable<IStringInterpolator> interpolators)
    {
        _interpolators = interpolators;
    }

    public async Task<string?> ResolveAsync(string? value, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(value)) return value;
        var s = value.Trim();
        // Under --edges-only an interpolator that reads the host is never asked, so the job learns nothing of the host, not
        // even whether a name is set there; a variable the remaining chain leaves unresolved is refused below.
        var edgesOnly = EdgesOnlyScope.IsArmed;
        var chain = edgesOnly ? _interpolators.Where(i => i is not IHostInterpolator).ToList() : _interpolators;

        // 1. File Indirection
        if (s.StartsWith('@'))
        {
            if (edgesOnly) throw new InvalidOperationException(EdgesOnlyScope.FileRefusal);
            s = await File.ReadAllTextAsync(s[1..], ct);
        }
        else
        {
            // 2. Exact Match (e.g. keyring://... without ${{}})
            foreach (var interpolator in chain)
            {
                var resolved = await interpolator.TryResolveAsync(s, ct);
                if (resolved != null)
                {
                    s = resolved;
                    break;
                }
            }
        }

        // 3. Inline Interpolation (e.g. ${{expr}}), through the grammar every reader of a job shares
        var tokens = VariableTokens.Of(s).ToList();
        if (tokens.Count == 0) return s;

        var sb = new StringBuilder();
        var lastIndex = 0;

        foreach (var token in tokens)
        {
            if (VariableTokens.Refusal(token.Expression) is { } refusal)
                throw new FormatException(refusal);

            sb.Append(s, lastIndex, token.Index - lastIndex);

            string? resolvedValue = null;
            foreach (var interpolator in chain)
            {
                resolvedValue = await interpolator.TryResolveAsync(token.Expression, ct);
                if (resolvedValue != null)
                {
                    break;
                }
            }

            if (resolvedValue is null && edgesOnly)
                throw new InvalidOperationException(EdgesOnlyScope.VariableRefusal(token.Expression));
            sb.Append(resolvedValue ?? s.Substring(token.Index, token.Length));
            lastIndex = token.Index + token.Length;
        }

        sb.Append(s, lastIndex, s.Length - lastIndex);
        return sb.ToString();
    }
}

/// <summary>
/// An interpolator whose answer comes from the host (its environment, its keyring): never asked inside <see cref="EdgesOnlyScope"/>.
/// </summary>
public interface IHostInterpolator : IStringInterpolator
{
}

/// <summary>
/// A run under <c>--edges-only</c>, ambient like <see cref="LaunchValues"/>: inside the scope an <c>@file</c> value and a
/// cursor's state file are refused, so a transformer's option (<c>--compute</c>, <c>--filter</c>, <c>--expand</c>) reads no file
/// of the host, and no
/// <see cref="IHostInterpolator"/> is asked, so a <c>${{NAME}}</c> that is neither a launch value nor a cursor is refused rather
/// than read from the host's environment or keyring. The command line arms it before the conversion resolves a <c>--job</c>
/// file's values, and <c>JobExecutor</c> for the run itself. No other call sees the scope, whatever runs beside it.
/// </summary>
public static class EdgesOnlyScope
{
    private static readonly AsyncLocal<bool> Armed = new();

    public const string FileRefusal = "A file of the host (an @file value, a cursor's state file) is refused under --edges-only: the run reads its arrow:- edges and nothing else.";

    /// <summary>The refusal of a variable the run's own values leave unresolved: it names the variable, never a value.</summary>
    public static string VariableRefusal(string name) =>
        "${{" + name + "}} is refused under --edges-only: the run reads no variable of its host, only a launch value or a cursor.";

    internal static bool IsArmed => Armed.Value;

    /// <summary>Refuses every file and every host variable resolved inside the returned scope.</summary>
    public static IDisposable Arm()
    {
        var previous = Armed.Value;
        Armed.Value = true;
        return new Restorer(previous);
    }

    private sealed class Restorer(bool previous) : IDisposable
    {
        public void Dispose() => Armed.Value = previous;
    }
}
