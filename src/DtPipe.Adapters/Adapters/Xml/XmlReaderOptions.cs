using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Xml;

[Description("Reads data from an XML file, extracting records at a configurable element path.")]
[ComponentHelp(
    usageNotes: "Connection string is a file path ending in '.xml' (or the 'xml:' prefix; '-' for stdin). '--path' selects the records to extract: an absolute slash-path (e.g. '/Catalog/Product') matches from the root, while a leading '//' (e.g. '//Product') recursively matches by element name at any depth; attributes are exposed as fields prefixed with '--xml-attribute-prefix' (default '_').",
    examples: new[] {
        "main:\n  input: \"catalog.xml\"\n  provider-options:\n    xml:\n      path: \"//Product\"\n      auto-column-types: true\n  output: \"<adapter-prefix>:<target>\""
    })]
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
