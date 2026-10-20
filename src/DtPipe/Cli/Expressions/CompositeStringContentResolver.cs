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
