using System.ComponentModel;
using DtPipe.Core.Options;
using DtPipe.Core.Attributes;

namespace DtPipe.Adapters.JsonL;

public class JsonLReaderOptions : IOptionSet, IHasSchemaOverride
{
	public static string Prefix => JsonLConstants.ProviderName;
	public static string DisplayName => "JsonL Reader";

	[Description("JSONL file path (use '-' for stdin)")]
	public string Jsonl { get; set; } = "";

	// Universal options — populated via JobDefinition, not direct CLI flags.
	// Use --path, --column-types, --max-sample, --encoding at the branch level.
	public string? Path { get; set; }
	public int MaxSample { get; set; } = 1000;
	public string ColumnTypes { get; set; } = "";
	public string Encoding { get; set; } = "UTF-8";

	/// <summary>Full Arrow schema JSON. Set by --export-job; consumed by --job. Not a CLI flag.</summary>
	public string Schema { get; set; } = "";
}
