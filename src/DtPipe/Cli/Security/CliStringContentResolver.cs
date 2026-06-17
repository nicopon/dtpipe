using System.Text.RegularExpressions;
using DtPipe.Core.Security;

namespace DtPipe.Cli.Security;

/// <summary>
/// Resolves string values through a sequential pipeline:
/// 1. @file → load file content (mutually exclusive with keyring://)
///    keyring://alias → load full value from OS keyring
/// 2. ${{ENV_VAR}} → substitute environment variables
///    ${{keyring://alias}} → substitute inline keyring secret
/// Steps are composable: a keyring value may itself contain ${{...}} placeholders.
/// </summary>
public sealed class CliStringContentResolver : DtPipe.Core.Security.IStringContentResolver
{
    private static readonly Regex InterpolationPattern =
        new(@"\$\{\{([^}]+)\}\}", RegexOptions.Compiled);

    private readonly ISecretsManager _secretsManager;

    public CliStringContentResolver(ISecretsManager secretsManager)
    {
        _secretsManager = secretsManager;
    }

    public async Task<string?> ResolveAsync(string? value, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(value)) return value;
        var s = value.Trim();

        if (s.TrimStart().StartsWith('@'))
            s = await File.ReadAllTextAsync(s.TrimStart()[1..], ct);
        else if (s.StartsWith("keyring://", StringComparison.OrdinalIgnoreCase))
            s = _secretsManager.GetSecret(s["keyring://".Length..]) ?? s;

        return InterpolationPattern.Replace(s, m =>
        {
            var expr = m.Groups[1].Value.Trim();
            if (expr.StartsWith("keyring://", StringComparison.OrdinalIgnoreCase))
                return _secretsManager.GetSecret(expr["keyring://".Length..]) ?? m.Value;
            if (expr.StartsWith("cursor://", StringComparison.OrdinalIgnoreCase))
                return ResolveCursorExpression(expr["cursor://".Length..]);
            return Environment.GetEnvironmentVariable(expr) ?? m.Value;
        });
    }

    private static string ResolveCursorExpression(string expression)
    {
        var envOverride = Environment.GetEnvironmentVariable("DTPIPE_CURSOR_OVERRIDE");
        if (!string.IsNullOrEmpty(envOverride))
        {
            return envOverride;
        }

        var parts = expression.Split('|', 2);
        var statePath = parts[0].Trim();
        var defaultValue = parts.Length > 1 ? parts[1].Trim() : "";

        var cursor = DtPipe.Core.Cursor.CursorStateStore.Read(statePath);
        return cursor?.Value ?? defaultValue;
    }
}
