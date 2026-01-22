using System.ComponentModel;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Oracle;

public enum OracleWriteStrategy
{
    Append,
    Truncate,
    DeleteThenInsert
}

public record OracleWriterOptions : IProviderOptions
{
    public static string Prefix => "oracle-writer";
    public static string DisplayName => "Oracle Writer Options";

    [Description("Target table name. Defaults to 'EXPORT_DATA'.")]
    public string Table { get; set; } = "EXPORT_DATA";

    [Description("Strategy for writing data: Append (default), Truncate, or DeleteThenInsert.")]
    public OracleWriteStrategy Strategy { get; set; } = OracleWriteStrategy.Append;

    [Description("Rows per batch for OracleBulkCopy. Default 5000. Set to 0 to use standard INSERT statements.")]
    public int BulkSize { get; set; } = 5000;
}
