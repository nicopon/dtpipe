using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Csv;

public class CsvReaderOptions : IOptionSet
{
	public static string Prefix => "csv";
	public static string DisplayName => "CSV Reader";

	[Description("CSV field separator")]
	public string Separator { get; set; } = ",";

	[Description("Whether the CSV file has a header row")]
	public bool HasHeader { get; set; } = true;

	[Description("File encoding (e.g., UTF-8, ISO-8859-1)")]
	public string Encoding { get; set; } = "UTF-8";

	[Description("Explicit column types, e.g. \"Id:uuid,Qty:int32,Price:double\". Supported: uuid, string, int32, int64, double, decimal, bool, datetime, datetimeoffset")]
	public string ColumnTypes { get; set; } = "";

	[Description("Automatically infer and apply column types from the first 100 rows (no --dry-run required). Prints the applied types.")]
	public bool AutoColumnTypes { get; set; } = false;
}
