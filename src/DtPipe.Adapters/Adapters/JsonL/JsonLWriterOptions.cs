using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.JsonL;

public class JsonLWriterOptions : IOptionSet
{
	public static string Prefix => JsonLConstants.ProviderName;
	public static string DisplayName => "JsonL Writer";

	[Description("File encoding (e.g., UTF-8)")]
	public string Encoding { get; set; } = "UTF-8";

	[Description("Whether to indent the JSON output (not recommended for JsonL)")]
	public bool Indented { get; set; } = false;
}
