using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Xml;

public class XmlReaderOptions : IOptionSet
{
	public static string Prefix => XmlConstants.ProviderName;
	public static string DisplayName => "XML Reader";

	[Description("XML file path (use '-' for stdin)")]
	public string Xml { get; set; } = "";

	[Description("XPath-like selector for record nodes (e.g., //Item or Root/Items/Item)")]
	public string Path { get; set; } = "//Item";

	[Description("File encoding (e.g., UTF-8, ISO-8859-1)")]
	public string Encoding { get; set; } = "UTF-8";

	[Description("Namespace mappings in prefix=uri format (comma separated)")]
	public string? Namespaces { get; set; }

	[Description("Prefix for XML attributes in the resulting data structure")]
	public string AttributePrefix { get; set; } = "_";

	[Description("File read buffer size in bytes")]
	public int BufferSize { get; set; } = 1024 * 1024;

	[Description("Use high-performance synchronous parsing on a background thread")]
	public bool FastMode { get; set; } = true;
}
