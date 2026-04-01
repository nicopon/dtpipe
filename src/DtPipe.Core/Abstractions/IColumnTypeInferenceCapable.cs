namespace DtPipe.Core.Abstractions;

/// <summary>
/// Implemented by readers that can infer column types from a sample of data.
/// Used during dry-run to suggest explicit --column-types hints to the user,
/// or to automatically apply inferred types when --auto-column-types is set.
/// </summary>
public interface IColumnTypeInferenceCapable
{
    /// <summary>
    /// Opens the source independently, reads up to <paramref name="sampleRows"/> rows,
    /// and returns a suggested column-type map (column name → type hint string).
    /// Only columns where the inferred type is more specific than "string" are included.
    /// </summary>
    Task<IReadOnlyDictionary<string, string>> InferColumnTypesAsync(int sampleRows, CancellationToken ct = default);

    /// <summary>
    /// When non-null after <c>OpenAsync</c>, types were automatically inferred and applied
    /// during opening (triggered by <c>--auto-column-types</c>).
    /// The value is the map of column name → type hint that was applied.
    /// </summary>
    IReadOnlyDictionary<string, string>? AutoAppliedTypes => null;
}
