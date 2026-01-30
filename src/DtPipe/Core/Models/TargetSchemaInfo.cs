namespace DtPipe.Core.Models;

/// <summary>
/// Information about the target schema (table or file).
/// </summary>
/// <param name="Columns">List of columns in the target schema</param>
/// <param name="Exists">Whether the target already exists</param>
/// <param name="RowCount">Approximate number of rows (null if unknown or not applicable)</param>
/// <param name="SizeBytes">Size in bytes (null if unknown or not applicable)</param>
/// <param name="PrimaryKeyColumns">List of primary key column names (null if no PK or unknown)</param>
public sealed record TargetSchemaInfo(
    IReadOnlyList<TargetColumnInfo> Columns,
    bool Exists,
    long? RowCount,
    long? SizeBytes,
    IReadOnlyList<string>? PrimaryKeyColumns
);

/// <summary>
/// Information about a single column in the target schema.
/// </summary>
/// <param name="Name">Column name</param>
/// <param name="NativeType">Native database type (e.g., "VARCHAR(100)", "INTEGER", "NUMBER(10)")</param>
/// <param name="InferredClrType">Equivalent CLR type (null if unable to infer)</param>
/// <param name="IsNullable">Whether the column allows NULL values</param>
/// <param name="IsPrimaryKey">Whether the column is part of the primary key</param>
/// <param name="IsUnique">Whether the column has a UNIQUE constraint</param>
/// <param name="MaxLength">Maximum length for string/binary types (null if not applicable)</param>
public sealed record TargetColumnInfo(
    string Name,
    string NativeType,
    Type? InferredClrType,
    bool IsNullable,
    bool IsPrimaryKey,
    bool IsUnique,
    int? MaxLength = null,
    int? Precision = null,
    int? Scale = null
);
