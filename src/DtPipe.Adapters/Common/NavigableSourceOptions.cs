using DtPipe.Core.Attributes;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Base options for navigable text sources (JSON, XML) that support path-based record extraction.
/// </summary>
public abstract class NavigableSourceOptions : TextSourceOptions
{
    [ComponentOption("--path", Description = "Navigation path (dot-path for JSON e.g. items.data, XPath for XML e.g. //Record)")]
    public string Path { get; set; } = "";
}
