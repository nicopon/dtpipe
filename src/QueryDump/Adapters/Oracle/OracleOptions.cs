using System.ComponentModel;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Oracle;

public record OracleOptions : IProviderOptions
{
    public static string Prefix => "ora";
    public static string DisplayName => "Oracle Reader";
    
    [Description("Fetch size in bytes (Oracle only)")]
    public int FetchSize { get; init; } = 1_048_576; // 1MB
}
