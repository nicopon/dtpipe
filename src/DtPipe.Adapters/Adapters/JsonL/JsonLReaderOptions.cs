using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Options;
using DtPipe.Core.Attributes;

namespace DtPipe.Adapters.JsonL;

public class JsonLReaderOptions : NavigableSourceOptions, IOptionSet, IHasSchemaOverride
{
	public static string Prefix => JsonLConstants.ProviderName;
	public static string DisplayName => "JsonL Reader";

	[Description("JSONL file path (use '-' for stdin)")]
	public string Jsonl { get; set; } = "";

	/// <summary>Full Arrow schema JSON. Set by --export-job; consumed by --job. Not a CLI flag.</summary>
	public string Schema { get; set; } = "";
}
