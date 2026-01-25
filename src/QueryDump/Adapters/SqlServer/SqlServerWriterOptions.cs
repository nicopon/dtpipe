using QueryDump.Core.Attributes;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.SqlServer;

public class SqlServerWriterOptions : IWriterOptions
{
    public static string Prefix => "mssql";
    public static string DisplayName => "SQL Server Writer Options";

    [CliOption("--mssql-table", Description = "Target table name. Defaults to 'ExportData'.")]
    public string Table { get; set; } = "ExportData";

    [CliOption("--mssql-strategy", Description = "Write strategy: Append, Truncate, or DeleteThenInsert.")]
    public SqlServerWriteStrategy Strategy { get; set; } = SqlServerWriteStrategy.Append;
    
    [CliOption("--mssql-bulk-size", Description = "Rows per batch for SqlBulkCopy.")]
    public int BulkSize { get; set; } = 5000;
}

public enum SqlServerWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert
}
