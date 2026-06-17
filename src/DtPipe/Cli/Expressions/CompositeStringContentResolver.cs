using System.Text;
using System.Text.RegularExpressions;
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
    private static readonly Regex InterpolationPattern =
        new(@"\$\{\{([^}]+)\}\}", RegexOptions.Compiled);

    private readonly IEnumerable<IStringInterpolator> _interpolators;

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

        // 3. Inline Interpolation (e.g. ${{expr}})
        var matches = InterpolationPattern.Matches(s);
        if (matches.Count == 0) return s;

        var sb = new StringBuilder();
        var lastIndex = 0;

        foreach (Match match in matches)
        {
            sb.Append(s, lastIndex, match.Index - lastIndex);
            
            var expr = match.Groups[1].Value.Trim();
            string? resolvedValue = null;

            foreach (var interpolator in _interpolators)
            {
                resolvedValue = await interpolator.TryResolveAsync(expr, ct);
                if (resolvedValue != null)
                {
                    break;
                }
            }

            sb.Append(resolvedValue ?? match.Value);
            lastIndex = match.Index + match.Length;
        }

        sb.Append(s, lastIndex, s.Length - lastIndex);
        return sb.ToString();
    }
}
