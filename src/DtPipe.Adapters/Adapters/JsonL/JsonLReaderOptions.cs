using System.ComponentModel;
using DtPipe.Core.Options;
using DtPipe.Core.Attributes;

namespace DtPipe.Adapters.JsonL;

public class JsonLReaderOptions : IOptionSet
{
	public static string Prefix => JsonLConstants.ProviderName;
	public static string DisplayName => "JsonL Reader";

	[Description("JSONL file path (use '-' for stdin)")]
	public string Jsonl { get; set; } = "";

	[ComponentOption("--json-path", Aliases = new[] { "--jsonl-path" }, Description = "Optional path to the root node (e.g. 'areas') for hierarchical JSON")]
	public string? Path { get; set; }

	[ComponentOption("--json-max-sample", Aliases = new[] { "--jsonl-max-sample" }, Description = "Maximum number of rows to sample for schema inference (default: 1000)")]
	public int MaxSample { get; set; } = 1000;

	[ComponentOption("--json-column-types", Aliases = new[] { "--jsonl-column-types" }, Description = "Explicit column types, e.g. \"Id:uuid,Count:int64\". Supported: uuid, string, int32, int64, double, decimal, bool, datetime, datetimeoffset")]
	public string ColumnTypes { get; set; } = "";

	/// <summary>Full Arrow schema as compact JSON (set by --schema-load or --export-job; bypasses all inference).</summary>
	[Description("Full Arrow schema as compact JSON — set automatically by --schema-load or --export-job.")]
	public string SchemaJson { get; set; } = "";

	[Description("File encoding (e.g., UTF-8, ISO-8859-1)")]
	public string Encoding { get; set; } = "UTF-8";
}
