using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Arrow;

public class ArrowWriterOptions : IOptionSet
{
	public static string Prefix => ArrowConstants.ProviderName;
	public static string DisplayName => "Arrow Writer";

    [Description("Internal buffer size for column batching")]
    public int BatchSize { get; set; } = 10000;
}
