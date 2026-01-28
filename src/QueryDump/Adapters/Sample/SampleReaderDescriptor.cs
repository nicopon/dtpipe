using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.Sample;

public class SampleReaderDescriptor : IProviderDescriptor<IStreamReader>
{
    public string ProviderName => SampleConstants.ProviderName;

    public Type OptionsType => typeof(SampleReaderOptions);

    public bool RequiresQuery => false;

    public bool CanHandle(string connectionString)
    {
        return connectionString.StartsWith("sample:", StringComparison.OrdinalIgnoreCase);
    }

    public IStreamReader Create(string connectionString, object options, DumpOptions context, IServiceProvider serviceProvider)
    {
        var sampleOptions = (SampleReaderOptions)options;
        
        string config = connectionString;
        if (config.StartsWith("sample:", StringComparison.OrdinalIgnoreCase))
        {
            config = config.Substring(7);
        }

        var parts = config.Split(';', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length > 0 && long.TryParse(parts[0], out long count))
        {
            sampleOptions.RowCount = count;
        }

        // Parse custom columns if any
        if (parts.Length > 1)
        {
            for (int i = 1; i < parts.Length; i++)
            {
                var kvp = parts[i].Split('=');
                if (kvp.Length == 2)
                {
                    string name = kvp[0].Trim();
                    string typeStr = kvp[1].Trim().ToLowerInvariant();
                    Type type = typeStr switch
                    {
                        "int" => typeof(int),
                        "long" => typeof(long),
                        "bool" => typeof(bool),
                        "double" => typeof(double),
                        "date" => typeof(DateTime),
                        "guid" => typeof(Guid),
                        _ => typeof(string)
                    };
                    sampleOptions.ColumnDefinitions.Add(new SampleColumnDef { Name = name, Type = type });
                }
            }
        }

        // Default if no columns specified
        if (sampleOptions.ColumnDefinitions.Count == 0)
        {
            sampleOptions.ColumnDefinitions.Add(new SampleColumnDef { Name = "dummy", Type = typeof(string) });
        }

        return new SampleReader(
            connectionString,
            context.Query ?? "",
            (SampleReaderOptions)options);
    }
}
