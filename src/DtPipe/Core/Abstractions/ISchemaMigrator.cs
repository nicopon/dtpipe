using DtPipe.Core.Validation;

namespace DtPipe.Core.Abstractions;

/// <summary>
/// Interface for data writers that support automatic schema evolution.
/// </summary>
public interface ISchemaMigrator
{
    /// <summary>
    /// Attempts to migrate the target schema based on the compatibility report.
    /// Typically adds missing columns.
    /// </summary>
    ValueTask MigrateSchemaAsync(SchemaCompatibilityReport report, CancellationToken ct);
}
