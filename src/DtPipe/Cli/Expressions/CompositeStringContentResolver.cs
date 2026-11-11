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

        // 1. File Indirection
        if (s.StartsWith('@'))
        {
            if (EdgesOnlyScope.IsArmed) throw new InvalidOperationException(EdgesOnlyScope.FileRefusal);
            s = await File.ReadAllTextAsync(s[1..], ct);
        }
        else
        {
            // 2. Exact Match (e.g. keyring://... without ${{}})
            foreach (var interpolator in _interpolators)
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
            foreach (var interpolator in _interpolators)
            {
                resolvedValue = await interpolator.TryResolveAsync(token.Expression, ct);
                if (resolvedValue != null)
                {
                    break;
                }
            }

            sb.Append(resolvedValue ?? s.Substring(token.Index, token.Length));
            lastIndex = token.Index + token.Length;
        }

        sb.Append(s, lastIndex, s.Length - lastIndex);
        return sb.ToString();
    }
}

/// <summary>
/// A run under <c>--edges-only</c>, ambient like <see cref="LaunchValues"/>: inside the scope an <c>@file</c> value is
/// refused, so a transformer's option (<c>--compute</c>, <c>--filter</c>, <c>--expand</c>) reads no file of the host. The
/// command line arms it before the conversion, which resolves a <c>--job</c> file's values, and <c>JobExecutor</c> for the
/// run itself. No other call sees the scope, whatever runs beside it.
/// </summary>
public static class EdgesOnlyScope
{
    private static readonly AsyncLocal<bool> Armed = new();

    public const string FileRefusal = "An @file value is refused under --edges-only: the run reads its arrow:- edges and nothing else.";

    internal static bool IsArmed => Armed.Value;

    /// <summary>Refuses every <c>@file</c> resolved inside the returned scope.</summary>
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
