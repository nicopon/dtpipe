namespace DtPipe.Core.Expressions;

/// <summary>
/// Defines a specialized strategy for resolving custom string expressions (e.g. ${{cursor://...}}, ${{ENV_VAR}}).
/// </summary>
public interface IStringInterpolator
{
    /// <summary>
    /// Attempts to resolve an interpolation expression. 
    /// If the expression is not supported by this interpolator, it must return null.
    /// </summary>
    /// <param name="expression">The expression matched inside ${{...}} (e.g., "keyring://my-secret")</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>The resolved string, or null if the interpolator cannot handle this expression.</returns>
    Task<string?> TryResolveAsync(string expression, CancellationToken ct);
}
