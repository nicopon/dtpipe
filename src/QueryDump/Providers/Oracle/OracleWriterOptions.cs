using System.ComponentModel;
using QueryDump.Core.Options;

namespace QueryDump.Providers.Oracle;

public enum OracleWriteStrategy
{
    Append,
    Truncate,
    Recreate
}

public record OracleWriterOptions : IProviderOptions
{
    public static string Prefix => "oracle-writer";
    public static string DisplayName => "Oracle Writer Options";

    [Description("Target table name. Defaults to 'EXPORT_DATA'.")]
    public string Table { get; set; } = "EXPORT_DATA";

    [Description("Strategy for writing data: Append (default), Truncate, or Recreate.")]
    public OracleWriteStrategy Strategy { get; set; } = OracleWriteStrategy.Append;

    [Description("Rows per batch for OracleBulkCopy. Default 5000.")]
    public int BulkSize { get; set; } = 5000;
}
