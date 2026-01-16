namespace QueryDump.Core;

/// <summary>
/// Information about a database column.
/// </summary>
public sealed record ColumnInfo(string Name, Type ClrType, bool IsNullable);
