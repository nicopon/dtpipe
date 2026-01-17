namespace QueryDump.Core;

/// <summary>
/// Information about a database column.
/// </summary>
/// <param name="Name">Column name</param>
/// <param name="ClrType">CLR type of the column</param>
/// <param name="IsNullable">Whether the column is nullable</param>
/// <param name="IsVirtual">Virtual columns are not exported (used for intermediate values)</param>
public sealed record ColumnInfo(string Name, Type ClrType, bool IsNullable, bool IsVirtual = false);
