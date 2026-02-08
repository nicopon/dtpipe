namespace DtPipe.Core.Models;

/// <summary>
/// Information about a database column.
/// </summary>
/// <param name="Name">Column name</param>
/// <param name="ClrType">CLR type of the column</param>
/// <param name="IsNullable">Whether the column is nullable</param>
/// <param name="IsVirtual">Virtual columns are not exported (used for intermediate values)</param>
/// <param name="IsCaseSensitive">Whether the column name requires case-sensitive handling (must be quoted)</param>
/// <param name="OriginalName">Original column name if normalization occurred (e.g. from UPPERCASE to Normal case)</param>
public sealed record PipeColumnInfo(
    string Name, 
    Type ClrType, 
    bool IsNullable,
    bool IsCaseSensitive = false,
    string? OriginalName = null
);
