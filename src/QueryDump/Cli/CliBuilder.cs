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

        // Custom --help option to show grouped help
        var customHelpOption = new Option<bool>("--help", "-h", "-?")
        {
            Description = "Show help and usage information"
        };

        // Custom --null option (separate from auto-generated options)
        var nullOption = new Option<string[]>("--null")
        {
            Description = "Column(s) to set to null (repeatable)",
            Arity = ArgumentArity.ZeroOrMore,
            AllowMultipleArgumentsPerToken = true
        };

        // Custom --fake option (user-friendly alias for --fake-mappings)
        var fakeOption = new Option<string[]>("--fake")
        {
            Description = "Column:faker.method mapping (repeatable, e.g. 'NAME:name.firstname')",
            Arity = ArgumentArity.ZeroOrMore,
            AllowMultipleArgumentsPerToken = true
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
        rootCommand.Options.Add(customHelpOption);
        rootCommand.Options.Add(nullOption);
        rootCommand.Options.Add(fakeOption);

        // Collect core options for help grouping
        var coreOptions = new List<Option> 
        { 
            connectionOption, providerOption, queryOption, outputOption,
            connectionTimeoutOption, queryTimeoutOption, batchSizeOption 
        };

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
            // Handle --help with grouped output
            var showHelp = parseResult.GetValue(customHelpOption);
            if (showHelp)
            {
                HelpPrinter.PrintGroupedHelp(rootCommand, coreOptions, dynamicOptions, fakeOption, nullOption, fakeListOption);
                return 0;
            }
            
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
                
                // Special handling: inject --fake and --null values into FakeOptions
                if (optionType == typeof(FakeOptions))
                {
                    var fakeOptions = (FakeOptions)boundOptions;
                    
                    // Bind --fake option
                    var fakeMappings = parseResult.GetValue(fakeOption);
                    if (fakeMappings is not null && fakeMappings.Length > 0)
                    {
                        fakeOptions = fakeOptions with { Mappings = fakeMappings.ToList() };
                    }
                    
                    // Bind --null option
                    var nullColumns = parseResult.GetValue(nullOption);
                    if (nullColumns is not null && nullColumns.Length > 0)
                    {
                        fakeOptions = fakeOptions with { NullColumns = nullColumns.ToList() };
                    }
                    
                    boundOptions = fakeOptions;
                }
                
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

    /// <summary>
    /// Prints grouped help output organized by category.
    /// </summary>
    public static void PrintGroupedHelp(IOptionTypeProvider optionTypeProvider)
    {
        Console.WriteLine("Description:");
        Console.WriteLine("  QueryDump - Export database data to Parquet or CSV (DuckDB-optimized)");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  querydump [options]");
        Console.WriteLine();
        
        // Core Options
        Console.WriteLine("Core Options:");
        Console.WriteLine("  -c, --connection <connection>            Connection string (or env var)");
        Console.WriteLine("  -p, --provider <provider>                Database provider [default: auto]");
        Console.WriteLine("  -q, --query <query>                      SQL query (SELECT only)");
        Console.WriteLine("  -o, --output <output>                    Output file (.parquet or .csv)");
        Console.WriteLine("  --connection-timeout <seconds>           Connection timeout [default: 10]");
        Console.WriteLine("  --query-timeout <seconds>                Query timeout [default: 0]");
        Console.WriteLine("  -b, --batch-size <rows>                  Rows per batch [default: 50000]");
        Console.WriteLine();
        
        // Group dynamic options by category
        var allTypes = optionTypeProvider.GetAllOptionTypes().ToList();
        
        // Reader options
        var readers = allTypes.Where(t => typeof(IProviderOptions).IsAssignableFrom(t));
        foreach (var rt in readers)
        {
            var opts = CliOptionBuilder.GenerateOptionsForType(rt).ToList();
            if (opts.Count == 0) continue;
            var name = GetDisplayName(rt);
            Console.WriteLine($"{name} Options:");
            foreach (var opt in opts)
            {
                PrintOption(opt);
            }
            Console.WriteLine();
        }
        
        // Writer options
        var writers = allTypes.Where(t => typeof(IWriterOptions).IsAssignableFrom(t));
        foreach (var wt in writers)
        {
            var opts = CliOptionBuilder.GenerateOptionsForType(wt).ToList();
            if (opts.Count == 0) continue;
            var name = GetDisplayName(wt);
            Console.WriteLine($"{name} Options:");
            foreach (var opt in opts)
            {
                PrintOption(opt);
            }
            Console.WriteLine();
        }
        
        // Anonymization options (transformers + custom)
        Console.WriteLine("Anonymization Options:");
        Console.WriteLine("  --fake <COLUMN:faker.method>             Column to faker mapping (repeatable)");
        Console.WriteLine("  --null <COLUMN>                          Column(s) to set to null (repeatable)");
        Console.WriteLine("  --fake-locale <locale>                   Bogus locale [default: en]");
        Console.WriteLine("  --fake-seed <seed>                       Seed for reproducible data");
        Console.WriteLine("  --fake-list                              List available fakers");
        Console.WriteLine();
        
        // Standard options
        Console.WriteLine("Other Options:");
        Console.WriteLine("  -?, -h, --help                           Show this help");
        Console.WriteLine("  --version                                Show version");
        Console.WriteLine();
    }

    private static void PrintOption(Option opt)
    {
        var name = opt.Name;
        var desc = opt.Description ?? "";
        Console.WriteLine($"  {name,-40} {desc}");
    }

    private static string GetDisplayName(Type optionType)
    {
        var prop = optionType.GetProperty("DisplayName", 
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        return prop?.GetValue(null)?.ToString() ?? optionType.Name.Replace("Options", "");
    }
}
