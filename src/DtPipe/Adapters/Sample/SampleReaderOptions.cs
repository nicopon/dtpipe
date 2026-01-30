using DtPipe.Core.Options;

namespace DtPipe.Adapters.Sample;

public record SampleReaderOptions : IProviderOptions
{
    public static string Prefix => SampleConstants.ProviderName;
    public static string DisplayName => "Sample Data Generator";

    public long RowCount { get; set; } = 100;
    public List<SampleColumnDef> ColumnDefinitions { get; set; } = new();
}

public class SampleColumnDef
{
    public string Name { get; set; } = "dummy";
    public Type Type { get; set; } = typeof(string);
}
