using Microsoft.Extensions.DependencyInjection;
using QueryDump.Cli;
using QueryDump.Core.Abstractions;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Core.Options;
using QueryDump.Cli.Infrastructure;
using QueryDump.Core.Pipelines;
using QueryDump.Transformers.Format;
using QueryDump.Transformers.Fake;
using QueryDump.Transformers.Null;
using QueryDump.Transformers.Overwrite;
using QueryDump.Transformers.Script;
using QueryDump.Adapters.Oracle;
using QueryDump.Adapters.SqlServer;
using QueryDump.Adapters.DuckDB;
using QueryDump.Adapters.Sqlite;
using QueryDump.Adapters.Csv;
using QueryDump.Adapters.Parquet;
using QueryDump.Adapters.PostgreSQL;
using QueryDump.Adapters.Sample;

using Serilog;
using Microsoft.Extensions.Logging;

namespace QueryDump;

class Program
{
    static async Task<int> Main(string[] args)
    {
        var services = new ServiceCollection();
        ConfigureServices(services);
        var serviceProvider = services.BuildServiceProvider();

        // Initialize Serilog with default console logger (or no-op if we prefer only explicit file log)
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .CreateLogger();

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
            await Log.CloseAndFlushAsync();
        }
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        // Serilog
        services.AddLogging(logging => 
        {
            logging.ClearProviders();
            logging.SetMinimumLevel(LogLevel.Debug);
        });

        // Configuration
        services.AddSingleton<OptionsRegistry>();
        services.AddSingleton(Spectre.Console.AnsiConsole.Console);
        
        // CLI
        services.AddSingleton<CliService>();
        
        // Reader Factories using Generic Descriptor Bridge
        RegisterReader<OracleReaderDescriptor>(services);
        RegisterReader<SqlServerReaderDescriptor>(services);
        RegisterReader<DuckDbReaderDescriptor>(services);
        RegisterReader<PostgreSqlReaderDescriptor>(services);
        RegisterReader<SqliteReaderDescriptor>(services);
        RegisterReader<CsvReaderDescriptor>(services);
        RegisterReader<ParquetReaderDescriptor>(services);
        RegisterReader<SampleReaderDescriptor>(services);
        
        // Writer Factories using Generic Descriptor Bridge
        RegisterWriter<Adapters.Csv.CsvWriterDescriptor>(services);
        RegisterWriter<Adapters.Parquet.ParquetWriterDescriptor>(services);
        RegisterWriter<Adapters.DuckDB.DuckDbWriterDescriptor>(services);
        RegisterWriter<Adapters.Oracle.OracleWriterDescriptor>(services);
        RegisterWriter<Adapters.Checksum.ChecksumWriterDescriptor>(services);
        RegisterWriter<PostgreSqlWriterDescriptor>(services);
        RegisterWriter<SqliteWriterDescriptor>(services);
        
        /* 
           Helper for DI Registration 
           (Inlined logic for clean reading, ideally moved to extension method)
        */
    }

    private static void RegisterWriter<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IDataWriter>, new()
    {
        services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(
            new TDesc(),
            sp.GetRequiredService<OptionsRegistry>(),
            sp
        ));
    }

    private static void RegisterReader<TDesc>(IServiceCollection services) where TDesc : class, IProviderDescriptor<IStreamReader>, new()
    {
        services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(
            new TDesc(),
            sp.GetRequiredService<OptionsRegistry>(),
            sp
        ));
        
        // Transformer Factories
        services.AddSingleton<IDataTransformerFactory, NullDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, OverwriteDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, FakeDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, FormatDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, Transformers.Mask.MaskDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, ScriptDataTransformerFactory>();
        
        // Export Service
        services.AddSingleton<ExportService>();
    }
}
