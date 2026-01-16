using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;
using QueryDump.Writers.Csv;
using QueryDump.Writers.Parquet;

namespace QueryDump.Writers;

public interface IDataWriterFactory
{
    IDataWriter Create(DumpOptions options);
    IEnumerable<Type> GetSupportedOptionTypes();
}



public class DataWriterFactory : IDataWriterFactory
{
    private readonly OptionsRegistry _registry;

    public DataWriterFactory(OptionsRegistry registry)
    {
        _registry = registry;
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return ComponentOptionsHelper.GetOptionsType<CsvDataWriter>();
        yield return ComponentOptionsHelper.GetOptionsType<ParquetDataWriter>();
    }

    public IDataWriter Create(DumpOptions options)
    {
        var extension = Path.GetExtension(options.OutputPath).ToLowerInvariant();

        return extension switch
        {
            ".parquet" => new ParquetDataWriter(options.OutputPath),
            ".csv" => new CsvDataWriter(options.OutputPath, _registry.Get<CsvOptions>()),
            _ => throw new ArgumentException($"Unsupported file format: {extension}. Use .parquet or .csv")
        };
    }
}
