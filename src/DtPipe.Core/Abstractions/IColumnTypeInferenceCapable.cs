namespace DtPipe.Core.Abstractions;

/// <summary>
/// Implemented by readers that can infer column types from a sample of data.
/// Used during dry-run to suggest explicit --column-types hints to the user.
/// Inference is advisory only — it never modifies the reader's runtime schema.
/// </summary>
public interface IColumnTypeInferenceCapable
{
    /// <summary>
    /// Opens the source independently, reads up to <paramref name="sampleRows"/> rows,
    /// and returns a suggested column-type map (column name → type hint string).
    /// Only columns where the inferred type is more specific than "string" are included.
    /// </summary>
    Task<IReadOnlyDictionary<string, string>> InferColumnTypesAsync(int sampleRows, CancellationToken ct = default);
}
