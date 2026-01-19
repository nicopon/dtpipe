using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Transformers.Format;
using QueryDump.Transformers.Fake;
using QueryDump.Transformers.Null;
using QueryDump.Transformers.Overwrite;
using QueryDump.Writers;
using QueryDump.Providers.Oracle;
using QueryDump.Providers.SqlServer;
using QueryDump.Providers.DuckDB;

namespace QueryDump;

class Program
{
    static async Task<int> Main(string[] args)
    {
        var services = new ServiceCollection();
        ConfigureServices(services);
        var serviceProvider = services.BuildServiceProvider();

        var cliService = serviceProvider.GetRequiredService<CliService>();
        var (rootCommand, printHelp) = cliService.Build();
        
        if (args.Length == 0 || args.Any(a => a == "--help" || a == "-h" || a == "-?"))
        {
            printHelp();
            return 0;
        }
        
        return await rootCommand.Parse(args).InvokeAsync();
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        // Configuration
        services.AddSingleton<OptionsRegistry>();
        
        // CLI
        services.AddSingleton<CliService>();
        
        // Reader Factories
        services.AddSingleton<IStreamReaderFactory, OracleReaderFactory>();
        services.AddSingleton<IStreamReaderFactory, SqlServerReaderFactory>();
        services.AddSingleton<IStreamReaderFactory, DuckDbReaderFactory>();
        
        // Writer Factories
        services.AddSingleton<IDataWriterFactory, Writers.Csv.CsvWriterFactory>();
        services.AddSingleton<IDataWriterFactory, Writers.Parquet.ParquetWriterFactory>();
        services.AddSingleton<IDataWriterFactory, Providers.DuckDB.DuckDbDataWriterFactory>();
        services.AddSingleton<IDataWriterFactory, Providers.Oracle.OracleDataWriterFactory>();
        
        // Transformer Factories
        services.AddSingleton<IDataTransformerFactory, NullDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, OverwriteDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, FakeDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, FormatDataTransformerFactory>();
        
        // Export Service
        services.AddSingleton<ExportService>();
    }
}
