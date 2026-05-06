using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Xml;

public class XmlReaderOptions : NavigableSourceOptions, IOptionSet, IHasSchemaOverride
{
	public static string Prefix => XmlConstants.ProviderName;
	public static string DisplayName => "XML Reader";

	[Description("XML file path (use '-' for stdin)")]
	public string Xml { get; set; } = "";

	/// <summary>Full Arrow schema JSON. Set by --export-job; consumed by --job. Not a CLI flag.</summary>
	public string Schema { get; set; } = "";

	[Description("Namespace mappings in prefix=uri format (comma separated)")]
	public string? Namespaces { get; set; }

	[Description("Prefix for XML attributes in the resulting data structure")]
	public string AttributePrefix { get; set; } = "_";

	[Description("File read buffer size in bytes")]
	public int BufferSize { get; set; } = 1024 * 1024;
}
