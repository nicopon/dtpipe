namespace DtPipe.Core.Models;

/// <summary>
/// Result of the primary key validation analysis.
/// </summary>
/// <param name="IsRequired">Whether a primary key is required by the write strategy</param>
/// <param name="RequestedKeys">The keys requested by the user (--key)</param>
/// <param name="ResolvedKeys">The keys resolved against the final schema (or null if resolution failed)</param>
/// <param name="TargetPrimaryKeys">The actual primary keys of the target table (for cross-validation)</param>
/// <param name="Errors">Validation errors (blocking)</param>
/// <param name="Warnings">Validation warnings (non-blocking, e.g. mismatches)</param>
public record KeyValidationResult(
    bool IsRequired,
    IReadOnlyList<string>? RequestedKeys,
    IReadOnlyList<string>? ResolvedKeys,
    IReadOnlyList<string>? TargetPrimaryKeys,
    IReadOnlyList<string>? Errors,
    IReadOnlyList<string>? Warnings = null
)
{
    public bool IsValid => (Errors == null || Errors.Count == 0);
}
