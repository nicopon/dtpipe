using System.CommandLine;
using System.CommandLine.Parsing;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Cli;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Writers;

namespace QueryDump.Cli;

public class CliService
{
    private readonly IEnumerable<ICliContributor> _contributors;
    private readonly IServiceProvider _serviceProvider;

    public CliService(
        IServiceProvider serviceProvider,
        IEnumerable<IReaderFactory> readerFactories,
        IEnumerable<ITransformerFactory> transformerFactories,
        IEnumerable<IWriterFactory> writerFactories)
    {
        _serviceProvider = serviceProvider;
        
        // Aggregate all contributors
        var list = new List<ICliContributor>();
        list.AddRange(readerFactories);
        list.AddRange(transformerFactories);
        list.AddRange(writerFactories);
        _contributors = list;
    }

    public (RootCommand, Action) Build()
    {
        // Core Options
        var connectionOption = new Option<string?>("--connection", "-c") { Description = "Connection string (or set *_CONNECTION_STRING env var)" };
        var providerOption = new Option<string>("--provider", "-p") { Description = "Database provider (auto, oracle, sqlserver, duckdb)", DefaultValueFactory = _ => "auto" };
        var queryOption = new Option<string?>("--query", "-q") { Description = "SQL query to execute (SELECT only)" };
        var outputOption = new Option<string?>("--output", "-o") { Description = "Output file path (.parquet or .csv)" };
        var connectionTimeoutOption = new Option<int>("--connection-timeout") { Description = "Connection timeout in seconds", DefaultValueFactory = _ => 10 };
        var queryTimeoutOption = new Option<int>("--query-timeout") { Description = "Query timeout in seconds (0 = no timeout)", DefaultValueFactory = _ => 0 };
        var batchSizeOption = new Option<int>("--batch-size", "-b") { Description = "Rows per output batch", DefaultValueFactory = _ => 50_000 };
        
        // Core Help Options
        var coreOptions = new List<Option> { connectionOption, providerOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption };

        var rootCommand = new RootCommand("QueryDump - Export database data to Parquet or CSV (DuckDB-optimized)");
        foreach(var opt in coreOptions) rootCommand.Options.Add(opt);

        // Add Contributor Options
        foreach (var contributor in _contributors)
        {
            foreach (var opt in contributor.GetCliOptions())
            {
                if (!rootCommand.Options.Any(o => o.Name == opt.Name))
                {
                    rootCommand.Options.Add(opt);
                }
            }
        }

        rootCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            // 1. Contributor Autonomous Handling (e.g. valid flags that cause early exit like --fake-list, --version custom)
            foreach (var contributor in _contributors)
            {
                var exitCode = await contributor.HandleCommandAsync(parseResult, cancellationToken);
                if (exitCode.HasValue)
                {
                    return exitCode.Value;
                }
            }

            // 2. Validate Core
            var query = parseResult.GetValue(queryOption);
            var output = parseResult.GetValue(outputOption);

            if (string.IsNullOrWhiteSpace(query) || string.IsNullOrWhiteSpace(output))
            {
                Console.Error.WriteLine("Options '--query' and '--output' are required.");
                return 1;
            }

            // 3. Bind Options
            var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
            foreach(var contributor in _contributors)
            {
                contributor.BindOptions(parseResult, registry);
            }

            // 4. Resolve Connection & Provider
            var provider = parseResult.GetValue(providerOption)!;
            var connection = parseResult.GetValue(connectionOption);
            
            connection = ConnectionHelper.ResolveConnection(connection, provider);
            
            if (string.IsNullOrWhiteSpace(connection))
            {
                Console.Error.WriteLine("Error: Connection string required.");
                return 1; // Exit code 1
            }

            if (string.Equals(provider, "auto", StringComparison.OrdinalIgnoreCase))
            {
                var detected = ProviderDetector.Detect(connection);
                if (detected == DatabaseProvider.Unknown)
                {
                     Console.Error.WriteLine("Error: Could not auto-detect provider.");
                     return 1;
                }
                 provider = detected switch
                {
                    DatabaseProvider.Oracle => "oracle",
                    DatabaseProvider.SqlServer => "sqlserver",
                    DatabaseProvider.DuckDB => "duckdb",
                    _ => "unknown"
                };
                Console.WriteLine($"Auto-detected provider: {provider}");
            }

            // 5. Run Export
             var options = new DumpOptions
            {
                Provider = provider,
                ConnectionString = connection,
                Query = query!,
                OutputPath = output!,
                ConnectionTimeout = parseResult.GetValue(connectionTimeoutOption),
                QueryTimeout = parseResult.GetValue(queryTimeoutOption),
                BatchSize = parseResult.GetValue(batchSizeOption)
            };

            var exportService = _serviceProvider.GetRequiredService<ExportService>();
             try
            {
                await exportService.RunExportAsync(options, cancellationToken);
                return 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"\nError: {ex.Message}");
                if (options.Provider == "duckdb" || Environment.GetEnvironmentVariable("DEBUG") == "1") Console.Error.WriteLine(ex.StackTrace);
                return 1;
            }
        });

        Action printHelp = () => PrintGroupedHelp(rootCommand, coreOptions, _contributors);
        return (rootCommand, printHelp);
    }

    private static void PrintGroupedHelp(RootCommand rootCommand, List<Option> coreOptions, IEnumerable<ICliContributor> contributors)
    {
        Console.WriteLine("Description:");
        Console.WriteLine("  QueryDump - Export database data to Parquet or CSV (DuckDB-optimized)");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  querydump [options]");
        Console.WriteLine();
        
        Console.WriteLine("Core Options:");
        foreach (var opt in coreOptions) PrintOption(opt);
        Console.WriteLine();

        var groups = contributors.GroupBy(c => c.Category).OrderBy(g => g.Key);
        
        foreach (var group in groups)
        {
            Console.WriteLine($"{group.Key}:");
            // Collect all options for this group
            var optionsPrinted = new HashSet<string>();
            foreach (var contributor in group)
            {
                foreach (var opt in contributor.GetCliOptions())
                {
                    if (optionsPrinted.Add(opt.Name))
                    {
                         PrintOption(opt);
                    }
                }
            }
            Console.WriteLine();
        }

        Console.WriteLine("Other Options:");
        Console.WriteLine("  -?, -h, --help                           Show this help");
        Console.WriteLine("  --version                                Show version");
        Console.WriteLine();
    }

    private static void PrintOption(Option opt)
    {
        var allAliases = new HashSet<string> { opt.Name };
        foreach (var alias in opt.Aliases) allAliases.Add(alias);
        
        var name = string.Join(", ", allAliases.OrderByDescending(a => a.Length));
        var desc = opt.Description ?? "";
        Console.WriteLine($"  {name,-40} {desc}");
    }
}
