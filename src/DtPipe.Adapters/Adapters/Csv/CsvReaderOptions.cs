using System.ComponentModel;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Csv;

public class CsvReaderOptions : IOptionSet
{
	public static string Prefix => "csv";
	public static string DisplayName => "CSV Reader";

	// Provider-specific options — keep their CLI flags.
	[Description("CSV field separator")]
	public string Separator { get; set; } = ",";

	[Description("Whether the CSV file has a header row")]
	public bool HasHeader { get; set; } = true;

	// Universal options — populated via JobDefinition, not direct CLI flags.
	// Use --column-types, --auto-column-types, --encoding at the branch level.
	public string ColumnTypes { get; set; } = "";
	public bool AutoColumnTypes { get; set; } = false;
	public string Encoding { get; set; } = "UTF-8";
}
