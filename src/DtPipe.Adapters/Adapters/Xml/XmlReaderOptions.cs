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

	[Description("Explicit column types, e.g. \"Id:int32,Meta.Id:uuid\". Supported: uuid, string, int32, int64, double, decimal, bool, datetime, datetimeoffset")]
	public string ColumnTypes { get; set; } = "";

	[Description("Automatically infer and apply column types from the first 100 rows (no --dry-run required)")]
	public bool AutoColumnTypes { get; set; } = false;

	/// <summary>Full Arrow schema as compact JSON (set by --schema-load or --export-job; bypasses all inference).</summary>
	[Description("Full Arrow schema as compact JSON — set automatically by --schema-load or --export-job.")]
	public string SchemaJson { get; set; } = "";
}
