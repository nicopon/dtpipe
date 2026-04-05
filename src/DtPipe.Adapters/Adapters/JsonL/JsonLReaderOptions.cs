using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.JsonL;

public class JsonLReaderOptions : IOptionSet
{
	public static string Prefix => JsonLConstants.ProviderName;
	public static string DisplayName => "JsonL Reader";

	[Description("JSONL file path (use '-' for stdin)")]
	public string Jsonl { get; set; } = "";

	[Description("File encoding (e.g., UTF-8, ISO-8859-1)")]
	public string Encoding { get; set; } = "UTF-8";
}
