namespace DtPipe.Core.Abstractions;

/// <summary>
/// Optional interface for data writers that support primary key validation.
/// Allows dry run to validate key specifications before actual execution.
/// </summary>
/// <remarks>
/// This interface is designed to be implemented alongside <see cref="ISchemaInspector"/>
/// by data writers that support key-based operations (Upsert, Ignore, Delete).
/// 
/// ARCHITECTURAL NOTE: This is an OPTIONAL interface. Writers that don't support
/// key-based operations (CSV, Parquet) do not need to implement this.
/// </remarks>
public interface IKeyValidator
{
    /// <summary>
    /// Returns the write strategy that determines if a primary key is required.
    /// </summary>
    /// <returns>
    /// Strategy name (e.g., "Upsert", "Ignore", "Recreate", "Append").
    /// Returns null if strategy is not applicable or unknown.
    /// </returns>
    /// <remarks>
    /// This should return a string representation of the strategy enum value.
    /// Examples:
    /// - PostgreSQL: "Upsert", "Ignore", "Recreate", "Truncate", "Append"
    /// - Oracle: "Upsert", "Ignore", "Recreate"
    /// - SQL Server: "Upsert", "Ignore", "Recreate", "Truncate"
    /// </remarks>
    string? GetWriteStrategy();
    
    /// <summary>
    /// Returns the primary key column names requested by the user (e.g., via --key option).
    /// </summary>
    /// <returns>
    /// List of key column names as specified by the user (not yet resolved/normalized).
    /// Returns null if no keys were specified.
    /// </returns>
    /// <remarks>
    /// IMPORTANT: This should return the RAW user input, not the normalized/resolved names.
    /// The dry run analyzer will perform resolution and validation.
    /// 
    /// Example: If user specified --key "Id,Name", this should return ["Id", "Name"]
    /// </remarks>
    IReadOnlyList<string>? GetRequestedPrimaryKeys();
    
    /// <summary>
    /// Returns true if the current strategy requires a primary key to function.
    /// </summary>
    /// <returns>
    /// True if a primary key is mandatory for the current operation.
    /// False if the operation can proceed without a key.
    /// </returns>
    /// <remarks>
    /// Strategies that typically require keys:
    /// - Upsert: Requires key to determine insert vs update
    /// - Ignore: Requires key to detect duplicates
    /// - Delete: Requires key to match rows
    /// 
    /// Strategies that don't require keys:
    /// - Recreate: Table is dropped and recreated
    /// - Truncate: Table is truncated then inserted
    /// - Append: Rows are blindly appended
    /// </remarks>
    bool RequiresPrimaryKey();
}
