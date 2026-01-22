using Microsoft.Extensions.DependencyInjection;
using QueryDump.Cli;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Transformers.Format;
using QueryDump.Transformers.Fake;
using QueryDump.Transformers.Null;
using QueryDump.Transformers.Overwrite;
using QueryDump.Adapters.Oracle;
using QueryDump.Adapters.SqlServer;
using QueryDump.Adapters.DuckDB;
using QueryDump.Adapters.Sqlite;
using QueryDump.Adapters.Csv;
using QueryDump.Adapters.Parquet;
using QueryDump.Adapters.PostgreSQL;

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
        
        // Ensure cursor is restored on Ctrl+C
        Console.CancelKeyPress += (_, _) => Console.CursorVisible = true;

        try 
        {
            return await rootCommand.Parse(args).InvokeAsync();
        }
        finally
        {
            // Ensure cursor is always visible upon exit, even after crash or Ctrl+C
            try { Console.CursorVisible = true; } catch { /* Ignore if unable to access console */ }
        }
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        // Configuration
        services.AddSingleton<OptionsRegistry>();
        services.AddSingleton(Spectre.Console.AnsiConsole.Console);
        
        // CLI
        services.AddSingleton<CliService>();
        
        // Reader Factories
        services.AddSingleton<IStreamReaderFactory, OracleReaderFactory>();
        services.AddSingleton<IStreamReaderFactory, SqlServerReaderFactory>();
        services.AddSingleton<IStreamReaderFactory, DuckDbReaderFactory>();
        services.AddSingleton<IStreamReaderFactory, PostgreSqlReaderFactory>();
        services.AddSingleton<IStreamReaderFactory, SqliteReaderFactory>();
        services.AddSingleton<IStreamReaderFactory, CsvReaderFactory>();
        services.AddSingleton<IStreamReaderFactory, ParquetReaderFactory>();
        
        // Writer Factories
        services.AddSingleton<IDataWriterFactory, Adapters.Csv.CsvWriterFactory>();
        services.AddSingleton<IDataWriterFactory, Adapters.Parquet.ParquetWriterFactory>();
        services.AddSingleton<IDataWriterFactory, Adapters.DuckDB.DuckDbDataWriterFactory>();
        services.AddSingleton<IDataWriterFactory, Adapters.Oracle.OracleDataWriterFactory>();
        services.AddSingleton<IDataWriterFactory, PostgreSqlDataWriterFactory>();
        services.AddSingleton<IDataWriterFactory, SqliteDataWriterFactory>();
        
        // Transformer Factories
        services.AddSingleton<IDataTransformerFactory, NullDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, OverwriteDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, FakeDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, FormatDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, Transformers.Mask.MaskDataTransformerFactory>();
        
        // Export Service
        services.AddSingleton<ExportService>();
    }
}
