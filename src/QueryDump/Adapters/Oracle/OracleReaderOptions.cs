using System.ComponentModel;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Oracle;

public record OracleReaderOptions : IProviderOptions
{
    public static string Prefix => OracleConstants.ProviderName;
    public static string DisplayName => "Oracle Reader Options";
    
    [Description("Fetch size in bytes (Oracle only)")]
    public int FetchSize { get; init; } = 1_048_576; // 1MB
}
