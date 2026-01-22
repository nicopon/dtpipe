using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Adapters.DuckDB;

public class DuckDbDataWriterFactory : BaseCliContributor, IDataWriterFactory
{
    public DuckDbDataWriterFactory(OptionsRegistry registry) : base(registry) { }

    public override string ProviderName => "duckdb-writer";
    public override string Category => "Writer Options";
    public string SupportedExtension => ".duckdb"; 

    public bool CanHandle(string outputPath)
    {
        return DuckDbConnectionHelper.CanHandle(outputPath);
    }

    public IDataWriter Create(DumpOptions options)
    {
        var connectionString = DuckDbConnectionHelper.GetConnectionString(options.OutputPath);
        
        if (!connectionString.Contains("DataSource=", StringComparison.OrdinalIgnoreCase) 
            && !connectionString.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase))
        {
            connectionString = $"Data Source={connectionString}";
        }

        return new DuckDbDataWriter(connectionString, Registry.Get<DuckDbWriterOptions>());
    }

    public override IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return typeof(DuckDbWriterOptions); 
    }
}
