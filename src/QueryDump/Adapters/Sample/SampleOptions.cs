using QueryDump.Core.Options;

namespace QueryDump.Adapters.Sample;

public class SampleOptions : IProviderOptions
{
    public static string Prefix => "sample";
    public static string DisplayName => "Sample Data Generator";

    public long RowCount { get; set; } = 100;
    public List<SampleColumnDef> ColumnDefinitions { get; set; } = new();
}

public class SampleColumnDef
{
    public string Name { get; set; } = "dummy";
    public Type Type { get; set; } = typeof(string);
}
