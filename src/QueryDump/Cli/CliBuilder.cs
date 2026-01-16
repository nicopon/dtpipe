using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Transformers.Fake;
using QueryDump.Writers;

namespace QueryDump.Cli;

public static class CliBuilder
{
    public static RootCommand Build(IServiceProvider serviceProvider)
    {
        // Core options
        var connectionOption = new Option<string?>("--connection", "-c")
        {
            Description = "Connection string (or set ORACLE_CONNECTION_STRING / MSSQL_CONNECTION_STRING / DUCKDB_CONNECTION_STRING env var)"
        };
        
        var providerOption = new Option<string>("--provider", "-p")
        {
            Description = "Database provider (auto, oracle, sqlserver, duckdb)",
            DefaultValueFactory = _ => "auto"
        };
        
        var queryOption = new Option<string?>("--query", "-q")
        {
            Description = "SQL query to execute (SELECT only, DDL/DML blocked)"
        };
        
        var outputOption = new Option<string?>("--output", "-o")
        {
            Description = "Output file path (.parquet or .csv)"
        };
        
        var connectionTimeoutOption = new Option<int>("--connection-timeout")
        {
            Description = "Connection timeout in seconds",
            DefaultValueFactory = _ => 10
        };
        
        var queryTimeoutOption = new Option<int>("--query-timeout")
        {
            Description = "Query timeout in seconds (0 = no timeout)",
            DefaultValueFactory = _ => 0
        };
        
        var batchSizeOption = new Option<int>("--batch-size", "-b")
        {
            Description = "Rows per output batch (Parquet RowGroup size / CSV buffer flush)",
            DefaultValueFactory = _ => 50_000
        };
        
        var fakeListOption = new Option<bool>("--fake-list")
        {
            Description = "List all available fake data generators and exit"
        };

        // Dynamic Options - discovered from factories
        var optionTypeProvider = serviceProvider.GetRequiredService<IOptionTypeProvider>();
        var dynamicOptions = new Dictionary<Type, List<Option>>();
        
        foreach (var optionType in optionTypeProvider.GetAllOptionTypes())
        {
            var options = CliOptionBuilder.GenerateOptionsForType(optionType).ToList();
            dynamicOptions[optionType] = options;
        }

        var rootCommand = new RootCommand("QueryDump - Export database data to Parquet or CSV (DuckDB-optimized)");
        
        rootCommand.Options.Add(connectionOption);
        rootCommand.Options.Add(providerOption);
        rootCommand.Options.Add(queryOption);
        rootCommand.Options.Add(outputOption);
        rootCommand.Options.Add(connectionTimeoutOption);
        rootCommand.Options.Add(queryTimeoutOption);
        rootCommand.Options.Add(batchSizeOption);
        rootCommand.Options.Add(fakeListOption);

        // Add dynamic options from all providers
        foreach (var options in dynamicOptions.Values)
        {
            foreach (var opt in options)
            {
                rootCommand.Options.Add(opt);
            }
        }

        rootCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            // Core args
            var provider = parseResult.GetValue(providerOption)!;
            var connection = parseResult.GetValue(connectionOption);
            var connectionTimeout = parseResult.GetValue(connectionTimeoutOption);
            var queryTimeout = parseResult.GetValue(queryTimeoutOption);
            var batchSize = parseResult.GetValue(batchSizeOption);

            // Handle informational flags
            var fakeList = parseResult.GetValue(fakeListOption);
            if (fakeList)
            {
                PrintFakerList();
                return 0;
            }

            var query = parseResult.GetValue(queryOption);
            var output = parseResult.GetValue(outputOption);

            if (string.IsNullOrWhiteSpace(query) || string.IsNullOrWhiteSpace(output))
            {
                Console.Error.WriteLine("Options '--query' and '--output' are required.");
                return 1;
            }

            // Bind Dynamic Options - dynamically from discovered types
            var registry = serviceProvider.GetRequiredService<OptionsRegistry>();
            
            foreach (var (optionType, cliOpts) in dynamicOptions)
            {
                var boundOptions = CliOptionBuilder.BindForType(optionType, parseResult, cliOpts);
                registry.RegisterByType(optionType, boundOptions);
            }

            // Resolve connection
            connection = ConnectionHelper.ResolveConnection(connection, provider);
            
            if (string.IsNullOrWhiteSpace(connection))
            {
                Console.Error.WriteLine($"Error: Connection string required. Use --connection or set appropriate environment variable.");
                return 1;
            }

            // Auto-detect provider
            if (string.Equals(provider, "auto", StringComparison.OrdinalIgnoreCase))
            {
                var detected = ProviderDetector.Detect(connection);
                if (detected == DatabaseProvider.Unknown)
                {
                    Console.Error.WriteLine("Error: Could not auto-detect provider. Please specify --provider.");
                    return 1;
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

            // Populate Legacy Options
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
                await exportService.RunExportAsync(options, cancellationToken);
                return 0;
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("SELECT") || ex.Message.Contains("Query must start"))
            {
                Console.Error.WriteLine($"\nSecurity Error: {ex.Message}");
                return 2;
            }
            catch (OperationCanceledException)
            {
                Console.Error.WriteLine("\nExport cancelled.");
                return 130;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"\nError: {ex.Message}");
                if (options.Provider == "duckdb") Console.Error.WriteLine(ex.StackTrace);
                return 1;
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
