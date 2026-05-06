using System.ComponentModel;
using DtPipe.Adapters.Common;
using DtPipe.Core.Options;

namespace DtPipe.Adapters.Csv;

public class CsvReaderOptions : TextSourceOptions, IOptionSet
{
	public static string Prefix => "csv";
	public static string DisplayName => "CSV Reader";

	[Description("CSV field separator")]
	public string Separator { get; set; } = ",";

	[Description("Whether the CSV file has a header row")]
	public bool HasHeader { get; set; } = true;
}
