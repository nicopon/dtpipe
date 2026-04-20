using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Xml;

public class XmlReaderOptions : IOptionSet, IHasSchemaOverride
{
	public static string Prefix => XmlConstants.ProviderName;
	public static string DisplayName => "XML Reader";

	[Description("XML file path (use '-' for stdin)")]
	public string Xml { get; set; } = "";

	// Universal options — populated via JobDefinition, not direct CLI flags.
	// Use --path, --column-types, --auto-column-types, --encoding at the branch level.
	public string Path { get; set; } = "//Item";
	public string Encoding { get; set; } = "UTF-8";
	public string ColumnTypes { get; set; } = "";
	public bool AutoColumnTypes { get; set; } = false;

	/// <summary>Full Arrow schema JSON. Set by --export-job; consumed by --job. Not a CLI flag.</summary>
	public string Schema { get; set; } = "";

	// Provider-specific options — keep their CLI flags.
	[Description("Namespace mappings in prefix=uri format (comma separated)")]
	public string? Namespaces { get; set; }

	[Description("Prefix for XML attributes in the resulting data structure")]
	public string AttributePrefix { get; set; } = "_";

	[Description("File read buffer size in bytes")]
	public int BufferSize { get; set; } = 1024 * 1024;
}
