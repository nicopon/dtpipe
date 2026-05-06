using DtPipe.Core.Attributes;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Common;

/// <summary>
/// Base options for text-based data sources (CSV, JSONL, XML).
/// Declares shared CLI flags so each text adapter does not need to repeat them.
/// </summary>
public abstract class TextSourceOptions : ISchemaPersistenceAware
{
    [ComponentOption("--encoding", Description = "Character encoding (UTF-8, ISO-8859-1…)")]
    public string Encoding { get; set; } = "UTF-8";

    [ComponentOption("--column-types", Description = "Explicit column types (e.g. Id:uuid,Count:int32)")]
    public string ColumnTypes { get; set; } = "";

    [ComponentOption("--auto-column-types", Description = "Infer column types from first sample rows")]
    public bool AutoColumnTypes { get; set; } = false;

    [ComponentOption("--max-sample", Description = "Max rows to sample for type inference (0 = reader default)")]
    public int MaxSample { get; set; } = 0;

    [ComponentOption("--schema-save", Description = "Save discovered schema to a .dtschema file after open")]
    public string? SchemaSave { get; set; }

    [ComponentOption("--schema-load", Description = "Load column types from a .dtschema file, bypassing inference")]
    public string? SchemaLoad { get; set; }
}
