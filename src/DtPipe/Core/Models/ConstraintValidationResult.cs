namespace DtPipe.Core.Models;

/// <summary>
/// Result of data constraint validation (NOT NULL, UNIQUE).
/// </summary>
public sealed record ConstraintValidationResult(
    IReadOnlyList<string> Errors,
    IReadOnlyList<string> Warnings
)
{
    public bool IsValid => Errors.Count == 0 && Warnings.Count == 0;
}
