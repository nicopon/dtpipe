using System.Text.RegularExpressions;

namespace DtPipe.Core.Expressions;

public interface IStringContentResolver
{
    Task<string?> ResolveAsync(string? value, CancellationToken ct = default);
}

/// <summary>
/// Resolves string values through a sequential pipeline:
/// 1. @file → load file content
/// 2. ${{ENV_VAR}} → substitute environment variables
/// keyring:// standalone and ${{keyring://...}} pass through unchanged (no SecretsManager in Core).
/// </summary>
public sealed class DefaultStringContentResolver : IStringContentResolver
{
    public static readonly DefaultStringContentResolver Instance = new();

    private static readonly Regex InterpolationPattern =
        new(@"\$\{\{([^}]+)\}\}", RegexOptions.Compiled);

    public async Task<string?> ResolveAsync(string? value, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(value)) return value;
        var s = value.TrimStart();

        if (s.StartsWith('@'))
            s = await File.ReadAllTextAsync(s[1..], ct);

        return InterpolationPattern.Replace(s, m =>
        {
            var expr = m.Groups[1].Value.Trim();
            if (expr.StartsWith("keyring://", StringComparison.OrdinalIgnoreCase))
                return m.Value;
            return Environment.GetEnvironmentVariable(expr) ?? m.Value;
        });
    }
}
