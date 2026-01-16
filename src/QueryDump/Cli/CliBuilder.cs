using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Providers.DuckDB;
using QueryDump.Providers.Oracle;
using QueryDump.Providers.SqlServer;
using QueryDump.Transformers.Fake;
using QueryDump.Writers.Csv;
using QueryDump.Writers.Parquet;

namespace QueryDump.Cli;

public static class CliBuilder
{
    public static RootCommand Build(IServiceProvider serviceProvider)
    {
        // Core options (Generic)
        var connectionOption = new Option<string?>(new[] { "--connection", "-c" }, "Connection string (or set ORACLE_CONNECTION_STRING / MSSQL_CONNECTION_STRING / DUCKDB_CONNECTION_STRING env var)");
        var providerOption = new Option<string>(new[] { "--provider", "-p" }, () => "auto", "Database provider (auto, oracle, sqlserver, duckdb)");
        var queryOption = new Option<string>(new[] { "--query", "-q" }, "SQL query to execute (SELECT only, DDL/DML blocked)");
        var outputOption = new Option<string>(new[] { "--output", "-o" }, "Output file path (.parquet or .csv)");
        // Execution options (kept generic for now until refactored into ExecutionOptions)
        var connectionTimeoutOption = new Option<int>("--connection-timeout", () => 10, "Connection timeout in seconds");
        var queryTimeoutOption = new Option<int>("--query-timeout", () => 0, "Query timeout in seconds (0 = no timeout)");
        var batchSizeOption = new Option<int>(new[] { "--batch-size", "-b" }, () => 50_000, "Rows per output batch (Parquet RowGroup size / CSV buffer flush)");
        
        var fakeListOption = new Option<bool>("--fake-list", "List all available fake data generators and exit");

        // Dynamic Options
        var oracleOptions = CliOptionBuilder.GenerateOptions<OracleOptions>().ToList();
        var sqlServerOptions = CliOptionBuilder.GenerateOptions<SqlServerOptions>().ToList();
        var duckDbOptions = CliOptionBuilder.GenerateOptions<DuckDbOptions>().ToList();
        var csvOptions = CliOptionBuilder.GenerateOptions<CsvOptions>().ToList();
        var parquetOptions = CliOptionBuilder.GenerateOptions<ParquetOptions>().ToList();
        var fakeOptions = CliOptionBuilder.GenerateOptions<FakeOptions>().ToList();

        var rootCommand = new RootCommand("QueryDump - Export database data to Parquet or CSV (DuckDB-optimized)")
        {
            connectionOption,
            providerOption,
            queryOption,
            outputOption,
            connectionTimeoutOption,
            queryTimeoutOption,
            batchSizeOption,
            fakeListOption
        };

        // Add dynamic options
        foreach (var opt in oracleOptions) rootCommand.AddOption(opt);
        foreach (var opt in sqlServerOptions) rootCommand.AddOption(opt);
        foreach (var opt in duckDbOptions) rootCommand.AddOption(opt);
        foreach (var opt in csvOptions) rootCommand.AddOption(opt);
        foreach (var opt in parquetOptions) rootCommand.AddOption(opt);
        foreach (var opt in fakeOptions) rootCommand.AddOption(opt);

        rootCommand.SetHandler(async (context) => 
        {
            var parseResult = context.ParseResult;
            
            // Core args
            var provider = parseResult.GetValueForOption(providerOption)!;
            var connection = parseResult.GetValueForOption(connectionOption);
            var connectionTimeout = parseResult.GetValueForOption(connectionTimeoutOption);
            var queryTimeout = parseResult.GetValueForOption(queryTimeoutOption);
            var batchSize = parseResult.GetValueForOption(batchSizeOption);

            // Handle informational flags
            var fakeList = parseResult.GetValueForOption(fakeListOption);
            if (fakeList)
            {
                PrintFakerList();
                context.ExitCode = 0;
                return;
            }

            if (parseResult.Tokens.Count == 0)
            {
                await rootCommand.InvokeAsync("--help");
                context.ExitCode = 1;
                return;
            }

            var query = parseResult.GetValueForOption(queryOption);
            var output = parseResult.GetValueForOption(outputOption);

            if (string.IsNullOrWhiteSpace(query) || string.IsNullOrWhiteSpace(output))
            {
                Console.Error.WriteLine("Options '--query' and '--output' are required.");
                context.ExitCode = 1;
                return;
            }

            // Bind Dynamic Options
            var registry = serviceProvider.GetRequiredService<OptionsRegistry>();
            
            registry.Register(CliOptionBuilder.Bind<OracleOptions>(parseResult, oracleOptions));
            registry.Register(CliOptionBuilder.Bind<SqlServerOptions>(parseResult, sqlServerOptions));
            registry.Register(CliOptionBuilder.Bind<DuckDbOptions>(parseResult, duckDbOptions));
            registry.Register(CliOptionBuilder.Bind<CsvOptions>(parseResult, csvOptions));
            registry.Register(CliOptionBuilder.Bind<ParquetOptions>(parseResult, parquetOptions));
            registry.Register(CliOptionBuilder.Bind<FakeOptions>(parseResult, fakeOptions));

            // Resolve connection
            connection = ConnectionHelper.ResolveConnection(connection, provider);
            
            if (string.IsNullOrWhiteSpace(connection))
            {
                Console.Error.WriteLine($"Error: Connection string required. Use --connection or set appropriate environment variable.");
                context.ExitCode = 1;
                return;
            }

            // Auto-detect provider
            if (string.Equals(provider, "auto", StringComparison.OrdinalIgnoreCase))
            {
                var detected = ProviderDetector.Detect(connection);
                if (detected == DatabaseProvider.Unknown)
                {
                    Console.Error.WriteLine("Error: Could not auto-detect provider. Please specify --provider.");
                    context.ExitCode = 1;
                    return;
                }
                
                provider = detected switch
                {
                    DatabaseProvider.Oracle => "oracle",
                    DatabaseProvider.SqlServer => "sqlserver",
                    DatabaseProvider.DuckDB => "duckdb",
                    DatabaseProvider.Postgres => "postgres",
                    DatabaseProvider.MySql => "mysql",
                    DatabaseProvider.Sqlite => "sqlite",
                    _ => "unknown"
                };
                Console.WriteLine($"Auto-detected provider: {provider}");
            }

            connection = ConnectionHelper.ApplyTimeouts(connection, provider, connectionTimeout);

            // Populate Legacy Options (Bridge to Phase 5)
            var options = new DumpOptions
            {
                Provider = provider,
                ConnectionString = connection,
                Query = query!,
                OutputPath = output!,
                ConnectionTimeout = connectionTimeout,
                QueryTimeout = queryTimeout,
                BatchSize = batchSize
            };

            var exportService = serviceProvider.GetRequiredService<ExportService>();

            try
            {
                await exportService.RunExportAsync(options, context.GetCancellationToken());
                context.ExitCode = 0;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("SELECT") || ex.Message.Contains("Query must start"))
            {
                Console.Error.WriteLine($"\nSecurity Error: {ex.Message}");
                context.ExitCode = 2;
            }
            catch (OperationCanceledException)
            {
                Console.Error.WriteLine("\nExport cancelled.");
                context.ExitCode = 130;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"\nError: {ex.Message}");
                if (options.Provider == "duckdb") Console.Error.WriteLine(ex.StackTrace);
                context.ExitCode = 1;
            }
        });

        return rootCommand;
    }

    private static void PrintFakerList()
    {
        var registry = new FakerRegistry();
        Console.WriteLine("Available fakers (use format: COLUMN:dataset.method)");
        Console.WriteLine();
        
        foreach (var (dataset, methods) in registry.ListAll())
        {
            Console.WriteLine($"{char.ToUpper(dataset[0])}{dataset[1..]}:");
            foreach (var (method, description) in methods)
            {
                var path = $"{dataset}.{method}".ToLowerInvariant();
                Console.WriteLine($"  {path,-30} {description}");
            }
            Console.WriteLine();
        }
        
        Console.WriteLine("Example: --fake \"NAME:name.firstname\" --fake \"EMAIL:internet.email\" --fake-locale fr");
    }
}
