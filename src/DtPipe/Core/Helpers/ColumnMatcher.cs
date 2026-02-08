using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Core.Helpers;

/// <summary>
/// Centralizes column name matching logic with dialect-aware normalization.
/// This is the SINGLE SOURCE OF TRUTH for resolving source column names to physical names.
/// </summary>
/// <remarks>
/// ARCHITECTURAL PRINCIPLE: This class must remain stateless and deterministic.
/// Given the same inputs, it must always produce the same output, matching exactly
/// how the target database would resolve the column name.
/// </remarks>
public static class ColumnMatcher
{
    /// <summary>
    /// Resolves a source column name to its physical representation in the target database.
    /// </summary>
    /// <param name="sourceName">The source column name (e.g., "Id", "MyColumn")</param>
    /// <param name="isCaseSensitive">Whether the source column is case-sensitive (quoted)</param>
    /// <param name="dialect">SQL dialect for normalization (null = no normalization)</param>
    /// <returns>Physical name that will be used in the database</returns>
    /// <remarks>
    /// CRITICAL: This method must produce the EXACT same output as the database would
    /// when receiving an INSERT/UPSERT command with this column name.
    /// 
    /// Examples:
    /// - PostgreSQL: ResolvePhysicalName("Id", false, pgDialect) → "id"
    /// - Oracle: ResolvePhysicalName("Id", false, oraDialect) → "ID"
    /// - PostgreSQL: ResolvePhysicalName("MyColumn", true, pgDialect) → "MyColumn" (quoted)
    /// - CSV: ResolvePhysicalName("Id", false, null) → "Id" (no normalization)
    /// </remarks>
    public static string ResolvePhysicalName(
        string sourceName,
        bool isCaseSensitive,
        ISqlDialect? dialect)
    {
        if (dialect == null)
        {
            // No dialect available (CSV, Parquet, etc.) - no normalization
            return sourceName;
        }
        
        // Case-sensitive columns or columns requiring quoting preserve exact case
        if (isCaseSensitive || dialect.NeedsQuoting(sourceName))
        {
            return sourceName;
        }
        
        // Unquoted columns are normalized according to dialect rules
        // PostgreSQL: lowercase, Oracle: UPPERCASE, SQL Server: no change
        return dialect.Normalize(sourceName);
    }
    
    /// <summary>
    /// Finds a target column that matches the source column name using dialect-aware rules.
    /// Returns null if no match is found (never throws exceptions).
    /// </summary>
    /// <typeparam name="T">Type of target column (PipeColumnInfo, TargetColumnInfo, string, etc.)</typeparam>
    /// <param name="sourceName">Source column name to find</param>
    /// <param name="isCaseSensitive">Whether source column is case-sensitive</param>
    /// <param name="targetColumns">List of target columns to search</param>
    /// <param name="getTargetName">Function to extract name from target column</param>
    /// <param name="dialect">SQL dialect for normalization</param>
    /// <returns>Matching column or null if not found</returns>
    /// <remarks>
    /// This method uses ORDINAL (case-sensitive) comparison because both source and target
    /// names are normalized to their physical representations first.
    /// 
    /// Example:
    /// Source: "Id" (case-insensitive) → normalized to "id" (PostgreSQL)
    /// Target columns: ["id", "name", "email"] → found "id" via Ordinal match
    /// </remarks>
    public static T? FindMatchingColumn<T>(
        string sourceName,
        bool isCaseSensitive,
        IReadOnlyList<T> targetColumns,
        Func<T, string> getTargetName,
        ISqlDialect? dialect) where T : class
    {
        var physicalName = ResolvePhysicalName(sourceName, isCaseSensitive, dialect);
        
        // CRITICAL: Use Ordinal comparison because physicalName is already normalized
        // to match the exact casing that the database uses
        return targetColumns.FirstOrDefault(col => 
            getTargetName(col).Equals(physicalName, StringComparison.Ordinal));
    }
    
    /// <summary>
    /// Case-insensitive fallback for scenarios without dialect information.
    /// </summary>
    /// <remarks>
    /// USE WITH CAUTION: This method should only be used when:
    /// 1. No SQL dialect is available (file-to-file operations)
    /// 2. As a compatibility layer for legacy code
    /// 
    /// Prefer dialect-aware matching via <see cref="FindMatchingColumn{T}"/> when possible.
    /// </remarks>
    public static T? FindMatchingColumnCaseInsensitive<T>(
        string sourceName,
        IReadOnlyList<T> targetColumns,
        Func<T, string> getTargetName) where T : class
    {
        return targetColumns.FirstOrDefault(col => 
            getTargetName(col).Equals(sourceName, StringComparison.OrdinalIgnoreCase));
    }
}
