namespace DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

/// <summary>
/// Interface for inspecting target schema before writing.
/// Implemented by database writers to enable schema compatibility analysis in dry-run mode.
/// </summary>
public interface ISchemaInspector
{
    /// <summary>
    /// Inspects the target schema (table, file).
    /// Returns null if the target does not exist yet.
    /// </summary>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Target schema information, or null if target doesn't exist</returns>
    Task<TargetSchemaInfo?> InspectTargetAsync(CancellationToken ct = default);
}
