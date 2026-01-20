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
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        IEnumerable<IDataWriterFactory> writerFactories)
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
        var inputOption = new Option<string?>("--input") { Description = "Input connection string or file path" };
        var queryOption = new Option<string?>("--query") { Description = "SQL query to execute (SELECT only)" };
        var outputOption = new Option<string?>("--output") { Description = "Output file path (.parquet or .csv)" };
        var connectionTimeoutOption = new Option<int>("--connection-timeout") { Description = "Connection timeout in seconds", DefaultValueFactory = _ => 10 };
        var queryTimeoutOption = new Option<int>("--query-timeout") { Description = "Query timeout in seconds (0 = no timeout)", DefaultValueFactory = _ => 0 };
        var batchSizeOption = new Option<int>("--batch-size") { Description = "Rows per output batch", DefaultValueFactory = _ => 50_000 };
        var unsafeQueryOption = new Option<bool>("--unsafe-query") { Description = "Bypass SQL query validation (allows DDL/DML - use with caution!)", DefaultValueFactory = _ => false };
        var dryRunOption = new Option<bool>("--dry-run") { Description = "Display query schema without exporting data", DefaultValueFactory = _ => false };
        var limitOption = new Option<int>("--limit") { Description = "Maximum rows to export (0 = unlimited)", DefaultValueFactory = _ => 0 };
        var jobOption = new Option<string?>("--job") { Description = "Path to YAML job file" };
        
        // Core Help Options
        var coreOptions = new List<Option> { inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption, unsafeQueryOption, dryRunOption, limitOption, jobOption };

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

            // 2. Handle Job File or CLI args
            var jobFile = parseResult.GetValue(jobOption);
            JobDefinition job;

            if (!string.IsNullOrWhiteSpace(jobFile))
            {
                // Load from job file
                try
                {
                    job = JobFileParser.Parse(jobFile);
                    Console.Error.WriteLine($"Loaded job file: {jobFile}");
                    
                    // CLI args override job file values
                    var cliQuery = parseResult.GetValue(queryOption);
                    var cliOutput = parseResult.GetValue(outputOption);
                    var cliInput = parseResult.GetValue(inputOption);
                    
                    if (!string.IsNullOrWhiteSpace(cliQuery)) job = job with { Query = cliQuery };
                    if (!string.IsNullOrWhiteSpace(cliOutput)) job = job with { Output = cliOutput };
                    if (!string.IsNullOrWhiteSpace(cliInput)) job = job with { Input = cliInput };
                    if (parseResult.GetValue(limitOption) > 0) job = job with { Limit = parseResult.GetValue(limitOption) };
                    if (parseResult.GetValue(dryRunOption)) job = job with { DryRun = true };
                    if (parseResult.GetValue(unsafeQueryOption)) job = job with { UnsafeQuery = true };
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"Error loading job file: {ex.Message}");
                    return 1;
                }
            }
            else
            {
                // CLI mode: validate required args
                var query = parseResult.GetValue(queryOption);
                var output = parseResult.GetValue(outputOption);
                var input = parseResult.GetValue(inputOption);

                if (string.IsNullOrWhiteSpace(query) || string.IsNullOrWhiteSpace(output))
                {
                    Console.Error.WriteLine("Options '--query' and '--output' are required (or use --job).");
                    return 1;
                }

                if (string.IsNullOrWhiteSpace(input))
                {
                    Console.Error.WriteLine("Error: --input is required.");
                    return 1;
                }

                job = new JobDefinition
                {
                    Input = input,
                    Query = query,
                    Output = output,
                    ConnectionTimeout = parseResult.GetValue(connectionTimeoutOption),
                    QueryTimeout = parseResult.GetValue(queryTimeoutOption),
                    BatchSize = parseResult.GetValue(batchSizeOption),
                    UnsafeQuery = parseResult.GetValue(unsafeQueryOption),
                    DryRun = parseResult.GetValue(dryRunOption),
                    Limit = parseResult.GetValue(limitOption)
                };
            }

            // 3. Bind Options
            var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
            foreach(var contributor in _contributors)
            {
                contributor.BindOptions(parseResult, registry);
            }

            // 4. Resolve Provider from job.Input
            var readerFactories = _contributors.OfType<IStreamReaderFactory>().ToList();

            // Auto-detect provider
            var detectedFactory = readerFactories.FirstOrDefault(f => f.CanHandle(job.Input));
            
            if (detectedFactory == null)
            {
                 Console.Error.WriteLine($"Error: Could not detect provider for input: '{job.Input}'.");
                 Console.Error.WriteLine("Please use a known prefix (e.g. 'duckdb:', 'oracle:', 'mssql:').");
                 return 1;
            }
            
            var provider = detectedFactory.ProviderName;
            Console.WriteLine($"Auto-detected provider: {provider}");

            // 5. Validate SQL query for safety
            try
            {
                SqlQueryValidator.Validate(job.Query, job.UnsafeQuery);
            }
            catch (InvalidOperationException ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
                return 1;
            }

            // 6. Build DumpOptions from JobDefinition
            var options = new DumpOptions
            {
                Provider = provider,
                ConnectionString = job.Input,
                Query = job.Query,
                OutputPath = job.Output,
                ConnectionTimeout = job.ConnectionTimeout,
                QueryTimeout = job.QueryTimeout,
                BatchSize = job.BatchSize,
                UnsafeQuery = job.UnsafeQuery,
                DryRun = job.DryRun,
                Limit = job.Limit
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
