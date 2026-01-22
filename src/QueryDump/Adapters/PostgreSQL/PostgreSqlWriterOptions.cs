using QueryDump.Core.Attributes;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.PostgreSQL;

public class PostgreSqlWriterOptions : IWriterOptions
{
    public static string Prefix => "pg";
    public static string DisplayName => "PostgreSQL Writer Options";

    // Writer Options
    [CliOption("--pg-table", Description = "Target table name for PostgreSQL export")]
    public string Table { get; set; } = "Export";

    [CliOption("--pg-strategy", Description = "Write strategy: Append, Truncate, or DeleteThenInsert")]
    public PostgreSqlWriteStrategy Strategy { get; set; } = PostgreSqlWriteStrategy.Append;
    
    // Potential future options: UseCopy (bool), BatchSize (for Copy buffer)
}

public enum PostgreSqlWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert
}

