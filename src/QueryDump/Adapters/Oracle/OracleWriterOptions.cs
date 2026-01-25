using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Adapters.Oracle;

public enum OracleWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert
}

public record OracleWriterOptions : IProviderOptions
{
    public static string Prefix => "oracle";
    public static string DisplayName => "Oracle Writer Options";

    [CliOption("--ora-table", Description = "Target table name. Defaults to 'EXPORT_DATA'.")]
    public string Table { get; set; } = "EXPORT_DATA";

    [CliOption("--ora-strategy", Description = "Strategy for writing data: Append (default), Truncate, or DeleteThenInsert.")]
    public OracleWriteStrategy Strategy { get; set; } = OracleWriteStrategy.Append;

    [CliOption("--ora-bulk-size", Description = "Rows per batch for OracleBulkCopy. Default 5000. Set to 0 to use standard INSERT statements.")]
    public int BulkSize { get; set; } = 5000;
}
