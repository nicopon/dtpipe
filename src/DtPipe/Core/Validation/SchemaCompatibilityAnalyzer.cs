using DtPipe.Core.Models;

namespace DtPipe.Core.Validation;

/// <summary>
/// Analyzes compatibility between source schema and target schema.
/// </summary>
public static class SchemaCompatibilityAnalyzer
{
    /// <summary>
    /// Analyzes compatibility between source and target schemas.
    /// </summary>
    /// <param name="sourceSchema">Schema of the data being written (after transformations)</param>
    /// <param name="targetSchema">Schema of the target (null if target doesn't exist)</param>
    /// <param name="dialect">SQL Dialect for identifier matching (optional)</param>
    /// <returns>Compatibility report with warnings and errors</returns>
    public static SchemaCompatibilityReport Analyze(
        IReadOnlyList<PipeColumnInfo> sourceSchema,
        TargetSchemaInfo? targetSchema,
        DtPipe.Core.Abstractions.ISqlDialect? dialect = null)
    {
        var columns = new List<ColumnCompatibility>();
        var warnings = new List<string>();
        var errors = new List<string>();

        // If target doesn't exist, all columns are new (will be created)
        if (targetSchema is null || !targetSchema.Exists)
        {
            foreach (var srcCol in sourceSchema)
            {
                columns.Add(new ColumnCompatibility(
                    srcCol.Name,
                    srcCol,
                    null,
                    CompatibilityStatus.WillBeCreated,
                    "Column will be created in new table"));
            }
            
            return new SchemaCompatibilityReport(targetSchema, columns, warnings, errors);
        }

        // Build lookup for target columns
        // Target columns are PHYSICAL names (usually).
        // If we have a dialect, we should simulate how the DB resolves the source name to a physical name.
        // If no dialect, fallback to fuzzy OrdinalIgnoreCase.
        
        // We use a mutable list of target columns to track matched ones
        var remainingTargetCols = targetSchema.Columns.ToList();

        // Check each source column
        foreach (var srcCol in sourceSchema)
        {
            TargetColumnInfo? tgtCol = null;

            // CRITICAL: Use ColumnMatcher as single source of truth
            if (dialect != null)
            {
                tgtCol = Core.Helpers.ColumnMatcher.FindMatchingColumn(
                    srcCol.Name,
                    srcCol.IsCaseSensitive,
                    remainingTargetCols,
                    c => c.Name,
                    dialect);
            }
            else
            {
                // Fallback: Case Insensitive match when no dialect available
                tgtCol = Core.Helpers.ColumnMatcher.FindMatchingColumnCaseInsensitive(
                    srcCol.Name,
                    remainingTargetCols,
                    c => c.Name);
            }

            if (tgtCol != null)
            {
                var (status, message) = CheckColumnCompatibility(srcCol, tgtCol);
                columns.Add(new ColumnCompatibility(srcCol.Name, srcCol, tgtCol, status, message));
                
                if (status == CompatibilityStatus.TypeMismatch)
                {
                    errors.Add($"Column '{srcCol.Name}': {message}");
                }
                else if (status != CompatibilityStatus.Compatible)
                {
                    warnings.Add($"Column '{srcCol.Name}': {message}");
                }
                
                remainingTargetCols.Remove(tgtCol);
            }
            else
            {
                // Column missing in target
                columns.Add(new ColumnCompatibility(
                    srcCol.Name,
                    srcCol,
                    null,
                    CompatibilityStatus.MissingInTarget,
                    "Column does not exist in target - data will be lost"));
                errors.Add($"Column '{srcCol.Name}': Missing in target schema (Checked as '{(dialect != null ? (srcCol.IsCaseSensitive || dialect.NeedsQuoting(srcCol.Name) ? srcCol.Name : dialect.Normalize(srcCol.Name)) : srcCol.Name)}') - data will be lost unless table is recreated");
            }
        }

        // Check for extra columns in target
        foreach (var tgtCol in remainingTargetCols)
        {
            var status = tgtCol.IsNullable 
                ? CompatibilityStatus.ExtraInTarget 
                : CompatibilityStatus.ExtraInTargetNotNull;
                
            var message = tgtCol.IsNullable
                ? "Exists in target but not in source (will be NULL)"
                : "Exists in target but not in source - NOT NULL constraint may fail";
                
            columns.Add(new ColumnCompatibility(tgtCol.Name, null, tgtCol, status, message));
            
            if (!tgtCol.IsNullable)
            {
                errors.Add($"Column '{tgtCol.Name}': {message}");
            }
            else
            {
                warnings.Add($"Column '{tgtCol.Name}': {message}");
            }
        }

        // Add warning if table has existing data
        if (targetSchema.RowCount.HasValue && targetSchema.RowCount.Value > 0)
        {
            var sizeInfo = targetSchema.SizeBytes.HasValue 
                ? $" • {FormatSize(targetSchema.SizeBytes.Value)}"
                : "";
            warnings.Insert(0, $"Target table already contains {targetSchema.RowCount:N0} rows{sizeInfo}");
        }

        return new SchemaCompatibilityReport(targetSchema, columns, warnings, errors);
    }

    private static (CompatibilityStatus, string?) CheckColumnCompatibility(PipeColumnInfo source, TargetColumnInfo target)
    {
        // Check nullability conflict
        if (source.IsNullable && !target.IsNullable && !target.IsPrimaryKey)
        {
            return (CompatibilityStatus.NullabilityConflict, 
                $"Source is nullable but target has NOT NULL constraint");
        }

        // Check type compatibility
        if (target.InferredClrType is not null)
        {
            var srcType = Nullable.GetUnderlyingType(source.ClrType) ?? source.ClrType;
            var tgtType = Nullable.GetUnderlyingType(target.InferredClrType) ?? target.InferredClrType;

            // Direct match
            if (srcType == tgtType)
            {
                return (CompatibilityStatus.Compatible, null);
            }

            // Check for safe conversions
            if (IsNumericUpcast(srcType, tgtType))
            {
                return (CompatibilityStatus.Compatible, null);
            }

            // String to smaller string (truncation risk)
            if (srcType == typeof(string) && tgtType == typeof(string) && target.MaxLength.HasValue)
            {
                return (CompatibilityStatus.PossibleTruncation, 
                    $"Source String may exceed {target.NativeType} limit");
            }

            // Type mismatch
            return (CompatibilityStatus.TypeMismatch, 
                $"Type mismatch: {srcType.Name} → {target.NativeType}");
        }

        // Cannot determine - assume compatible with warning
        return (CompatibilityStatus.Compatible, null);
    }

    private static bool IsNumericUpcast(Type source, Type target)
    {
        // Safe numeric conversions
        var numericOrder = new Dictionary<Type, int>
        {
            [typeof(byte)] = 1,
            [typeof(short)] = 2,
            [typeof(int)] = 3,
            [typeof(long)] = 4,
            [typeof(float)] = 5,
            [typeof(double)] = 6,
            [typeof(decimal)] = 7
        };

        if (numericOrder.TryGetValue(source, out var srcOrder) &&
            numericOrder.TryGetValue(target, out var tgtOrder))
        {
            return tgtOrder >= srcOrder;
        }

        return false;
    }

    private static string FormatSize(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024 * 1024 * 1024) return $"{bytes / (1024.0 * 1024):F1} MB";
        return $"{bytes / (1024.0 * 1024 * 1024):F1} GB";
    }
}

/// <summary>
/// Result of schema compatibility analysis.
/// </summary>
/// <param name="TargetInfo">Target schema information (null if target doesn't exist)</param>
/// <param name="Columns">Column-by-column compatibility analysis</param>
/// <param name="Warnings">List of warning messages</param>
/// <param name="Errors">List of error messages</param>
public sealed record SchemaCompatibilityReport(
    TargetSchemaInfo? TargetInfo,
    IReadOnlyList<ColumnCompatibility> Columns,
    IReadOnlyList<string> Warnings,
    IReadOnlyList<string> Errors
)
{
    /// <summary>
    /// Returns true if there are no errors (warnings are acceptable).
    /// </summary>
    public bool IsCompatible => Errors.Count == 0;
}

/// <summary>
/// Compatibility information for a single column.
/// </summary>
/// <param name="ColumnName">Name of the column</param>
/// <param name="SourceColumn">Source column info (null if extra in target)</param>
/// <param name="TargetColumn">Target column info (null if missing in target or will be created)</param>
/// <param name="Status">Compatibility status</param>
/// <param name="Message">Optional message explaining the status</param>
public sealed record ColumnCompatibility(
    string ColumnName,
    PipeColumnInfo? SourceColumn,
    TargetColumnInfo? TargetColumn,
    CompatibilityStatus Status,
    string? Message
);

/// <summary>
/// Compatibility status for a column.
/// </summary>
public enum CompatibilityStatus
{
    /// <summary>Types match perfectly or with safe conversion.</summary>
    Compatible,
    
    /// <summary>Column will be created (target doesn't exist).</summary>
    WillBeCreated,
    
    /// <summary>Source string may be larger than target allows.</summary>
    PossibleTruncation,
    
    /// <summary>Incompatible types that may cause data loss or errors.</summary>
    TypeMismatch,
    
    /// <summary>Column exists in source but not in target.</summary>
    MissingInTarget,
    
    /// <summary>Column exists in target but not in source (nullable, will be NULL).</summary>
    ExtraInTarget,
    
    /// <summary>Column exists in target but not in source (NOT NULL - will fail).</summary>
    ExtraInTargetNotNull,
    
    /// <summary>Source allows NULL but target has NOT NULL constraint.</summary>
    NullabilityConflict
}
