using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Transformers.Clone;
using QueryDump.Transformers.Fake;
using QueryDump.Transformers.Null;
using QueryDump.Transformers.Static;
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
        services.AddSingleton<IReaderFactory, OracleReaderFactory>();
        services.AddSingleton<IReaderFactory, SqlServerReaderFactory>();
        services.AddSingleton<IReaderFactory, DuckDbReaderFactory>();
        
        // Writer Factories
        services.AddSingleton<IWriterFactory, Writers.Csv.CsvWriterFactory>();
        services.AddSingleton<IWriterFactory, Writers.Parquet.ParquetWriterFactory>();
        
        // Transformer Factories
        services.AddSingleton<ITransformerFactory, NullDataTransformerFactory>();
        services.AddSingleton<ITransformerFactory, StaticDataTransformerFactory>();
        services.AddSingleton<ITransformerFactory, FakeDataTransformerFactory>();
        services.AddSingleton<ITransformerFactory, CloneDataTransformerFactory>();
        
        // Export Service
        services.AddSingleton<ExportService>();
    }
}
