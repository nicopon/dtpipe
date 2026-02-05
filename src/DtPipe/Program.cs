using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Cli;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Cli.Infrastructure;
using DtPipe.Transformers.Format;
using DtPipe.Transformers.Fake;
using DtPipe.Transformers.Null;
using DtPipe.Transformers.Overwrite;
using DtPipe.Transformers.Script;
using DtPipe.Transformers.Mask;
using DtPipe.Transformers.Project;
using DtPipe.Adapters.Oracle;
using DtPipe.Adapters.SqlServer;
using DtPipe.Adapters.DuckDB;
using DtPipe.Adapters.Sqlite;
using DtPipe.Adapters.Csv;
using DtPipe.Adapters.Parquet;
using DtPipe.Adapters.PostgreSQL;
using DtPipe.Adapters.Sample;
using DtPipe.Adapters.Checksum;
using Serilog;
using Microsoft.Extensions.Logging;

namespace DtPipe;

class Program
{
    static async Task<int> Main(string[] args)
    {
        System.Globalization.CultureInfo.DefaultThreadCurrentCulture = System.Globalization.CultureInfo.InvariantCulture;
        System.Globalization.CultureInfo.DefaultThreadCurrentUICulture = System.Globalization.CultureInfo.InvariantCulture;

        var services = new ServiceCollection();
        ConfigureServices(services);
        var serviceProvider = services.BuildServiceProvider();

        // Initialize Serilog with default console logger (or no-op if we prefer only explicit file log)
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .CreateLogger();

        var jobService = serviceProvider.GetRequiredService<JobService>();
        var (rootCommand, printHelp) = jobService.Build();
        
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
        services.AddSingleton<Spectre.Console.IAnsiConsole>(sp => 
        {
            return Spectre.Console.AnsiConsole.Create(new Spectre.Console.AnsiConsoleSettings 
            { 
                Out = new Spectre.Console.AnsiConsoleOutput(Console.Error) 
            });
        });
        
        // CLI
        services.AddSingleton<JobService>();
        
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
        RegisterWriter<CsvWriterDescriptor>(services);
        RegisterWriter<ParquetWriterDescriptor>(services);
        RegisterWriter<DuckDbWriterDescriptor>(services);
        RegisterWriter<OracleWriterDescriptor>(services);
        RegisterWriter<ChecksumWriterDescriptor>(services);
        RegisterWriter<SqlServerWriterDescriptor>(services);
        RegisterWriter<PostgreSqlWriterDescriptor>(services);
        RegisterWriter<SqliteWriterDescriptor>(services);
        
        // Transformer Factories
        services.AddSingleton<IDataTransformerFactory, NullDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, OverwriteDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, FakeDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, FormatDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, MaskDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, ScriptDataTransformerFactory>();
        services.AddSingleton<IDataTransformerFactory, ProjectDataTransformerFactory>();
        
        // Export Service
        services.AddSingleton<ExportService>();
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
    }
}
