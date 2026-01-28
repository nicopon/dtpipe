using System.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using QueryDump.Configuration;
using QueryDump.Core.Abstractions;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Core.Options;
using QueryDump.Core.Pipelines;
using QueryDump.Core.Validation;
using Serilog;
using QueryDump.Cli.Infrastructure;

namespace QueryDump.Cli;

public class JobService
{
    private readonly IEnumerable<ICliContributor> _contributors;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILoggerFactory _loggerFactory;

    public JobService(
        IServiceProvider serviceProvider,
        ILoggerFactory loggerFactory,
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        IEnumerable<IDataWriterFactory> writerFactories)
    {
        _serviceProvider = serviceProvider;
        _loggerFactory = loggerFactory;
        
        // Aggregate all contributors
        var list = new List<ICliContributor>();
        list.AddRange(readerFactories.OfType<ICliContributor>());
        list.AddRange(transformerFactories.OfType<ICliContributor>());
        list.AddRange(writerFactories.OfType<ICliContributor>());
        _contributors = list;
    }

    public (RootCommand, Action) Build()
    {
        // Core Options
        var inputOption = new Option<string?>("--input") { Description = "Input connection string or file path" };
        var queryOption = new Option<string?>("--query") { Description = "SQL query to execute (SELECT only)" };
        var outputOption = new Option<string?>("--output") { Description = "Output file path or connection string" };
        var connectionTimeoutOption = new Option<int>("--connection-timeout") { Description = "Connection timeout in seconds", DefaultValueFactory = _ => 10 };
        var queryTimeoutOption = new Option<int>("--query-timeout") { Description = "Query timeout in seconds (0 = no timeout)", DefaultValueFactory = _ => 0 };
        var batchSizeOption = new Option<int>("--batch-size") { Description = "Rows per output batch", DefaultValueFactory = _ => 50_000 };
        var unsafeQueryOption = new Option<bool>("--unsafe-query") { Description = "Bypass SQL query validation (allows DDL/DML - use with caution!)", DefaultValueFactory = _ => false };
        var dryRunOption = new Option<int>("--dry-run") { Description = "Dry-run mode: display pipeline trace analysis (N = sample count, default 1)", DefaultValueFactory = _ => 0, Arity = ArgumentArity.ZeroOrOne };
        var limitOption = new Option<int>("--limit") { Description = "Maximum rows to export (0 = unlimited)", DefaultValueFactory = _ => 0 };
        var sampleRateOption = new Option<double>("--sample-rate") { Description = "Sampling probability (0.0 to 1.0). e.g. 0.1 for 10%", DefaultValueFactory = _ => 1.0 };
        var sampleSeedOption = new Option<int?>("--sample-seed") { Description = "Seed for reproducible sampling" };
        var jobOption = new Option<string?>("--job") { Description = "Path to YAML job file" };
        var exportJobOption = new Option<string?>("--export-job") { Description = "Export current configuration to YAML file and exit" };
        var logOption = new Option<string?>("--log") { Description = "Path to log file" };
        
        // Core Help Options
        var coreOptions = new List<Option> { inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption, unsafeQueryOption, dryRunOption, limitOption, sampleRateOption, sampleSeedOption, jobOption, exportJobOption, logOption };

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
            // 1. Contributor Autonomous Handling
            foreach (var contributor in _contributors)
            {
                var exitCode = await contributor.HandleCommandAsync(parseResult, cancellationToken);
                if (exitCode.HasValue) return exitCode.Value;
            }

            // 2. Build Job Definition
            var (job, jobExitCode) = RawJobBuilder.Build(
                parseResult,
                jobOption, inputOption, queryOption, outputOption, 
                connectionTimeoutOption, queryTimeoutOption, batchSizeOption, 
                unsafeQueryOption, limitOption, sampleRateOption, sampleSeedOption, logOption);

            if (jobExitCode != 0) return jobExitCode;

            // 3. Handle --export-job
            var exportJobPath = parseResult.GetValue(exportJobOption);
            if (!string.IsNullOrWhiteSpace(exportJobPath))
            {
                var factoryList = _contributors.OfType<IDataTransformerFactory>().ToList();
                var configs = RawJobBuilder.BuildTransformerConfigsFromCli(
                    Environment.GetCommandLineArgs(), 
                    factoryList, 
                    _contributors);
                
                job = job with { Transformers = configs };
                JobFileWriter.Write(exportJobPath, job);
                return 0;
            }

            // 4. Bind Options
            var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();

            // 4a. Hydrate from YAML ProviderOptions
            if (job.ProviderOptions != null)
            {
                // We need to map provider prefix -> option type
                // Contributors act as factory sources.
                foreach (var contributor in _contributors)
                {
                   if (contributor is IDataFactory factory && factory is IDataWriterFactory or IStreamReaderFactory)
                   {
                        // Match keys in ProviderOptions
                        foreach (var kvp in job.ProviderOptions)
                        {
                            var key = kvp.Key;
                            
                            // Check writers
                            if (contributor is IDataWriterFactory wFactory)
                            {
                                var optionsType = wFactory.GetSupportedOptionTypes().FirstOrDefault();
                                if (optionsType != null && IsPrefixMatch(optionsType, key)) 
                                {
                                     var instance = registry.Get(optionsType);
                                     ConfigurationBinder.Bind(instance, kvp.Value);
                                     registry.RegisterByType(optionsType, instance);
                                }
                            }
                            // Check readers
                            else if (contributor is IStreamReaderFactory rFactory)
                            {
                                var optionsType = rFactory.GetSupportedOptionTypes().FirstOrDefault();
                                if (optionsType != null && IsPrefixMatch(optionsType, key)) 
                                {
                                     var instance = registry.Get(optionsType);
                                     ConfigurationBinder.Bind(instance, kvp.Value);
                                     registry.RegisterByType(optionsType, instance);
                                }
                            }
                        }
                   }
                }
            }

            // 4. Bind Options
            foreach(var contributor in _contributors)
            {
                contributor.BindOptions(parseResult, registry);
            }

            // 5. Resolve Reader & Writer
            var readerFactories = _contributors.OfType<IStreamReaderFactory>().ToList();
            var (readerFactory, cleanedInput) = ResolveFactory(readerFactories, job.Input, "reader");
            job = job with { Input = cleanedInput };
            
            Console.WriteLine($"Auto-detected input source: {readerFactory.ProviderName}");

            var writerFactories = _contributors.OfType<IDataWriterFactory>().ToList();
            var (writerFactory, cleanedOutput) = ResolveFactory(writerFactories, job.Output, "writer");
            job = job with { Output = cleanedOutput };

            // 6. Validate Query
            if (readerFactory.RequiresQuery)
            {
                if (string.IsNullOrWhiteSpace(job.Query))
                {
                    Console.Error.WriteLine($"Error: A query is required for provider '{readerFactory.ProviderName}'. Use --query \"SELECT...\"");
                    return 1;
                }

                try
                {
                    SqlQueryValidator.Validate(job.Query, job.UnsafeQuery);
                }
                catch (InvalidOperationException ex)
                {
                    Console.Error.WriteLine($"Error: {ex.Message}");
                    return 1;
                }
            }

            // 7. Build Runtime Options
            var options = new DumpOptions
            {
                Provider = readerFactory.ProviderName,
                ConnectionString = job.Input,
                Query = job.Query,
                OutputPath = job.Output,
                ConnectionTimeout = job.ConnectionTimeout,
                QueryTimeout = job.QueryTimeout,
                BatchSize = job.BatchSize,
                UnsafeQuery = job.UnsafeQuery,
                DryRunCount = RawJobBuilder.ParseDryRunFromArgs(Environment.GetCommandLineArgs()),
                Limit = job.Limit,
                SampleRate = job.SampleRate,
                SampleSeed = job.SampleSeed,
                LogPath = job.LogPath
            };

            // 8. Configure Logging
            if (!string.IsNullOrEmpty(options.LogPath))
            {
                Serilog.Log.Logger = new Serilog.LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .WriteTo.File(options.LogPath)
                    .CreateLogger();
                _loggerFactory.AddSerilog();
            }

            // 9. Build Pipeline & Run
            var exportService = _serviceProvider.GetRequiredService<ExportService>();
            var transformerFactories = _contributors.OfType<IDataTransformerFactory>().ToList();
            List<IDataTransformer> pipeline;
            
            if (job.Transformers != null && job.Transformers.Count > 0)
            {
                pipeline = BuildPipelineFromYaml(job.Transformers, transformerFactories);
            }
            else
            {
                var pipelineBuilder = new TransformerPipelineBuilder(transformerFactories);
                pipeline = pipelineBuilder.Build(Environment.GetCommandLineArgs());
            }
            
             try
            {
                await exportService.RunExportAsync(options, cancellationToken, pipeline, readerFactory, writerFactory);
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

    /// <summary>
    /// Builds a transformer pipeline from YAML TransformerConfig list.
    /// Matches each config's Type to factory's TransformerType and calls CreateFromYamlConfig.
    /// </summary>
    private static List<IDataTransformer> BuildPipelineFromYaml(
        List<TransformerConfig> configs, 
        List<IDataTransformerFactory> factories)
    {
        var pipeline = new List<IDataTransformer>();
        
        foreach (var config in configs)
        {
            var factory = factories.FirstOrDefault(f => 
                f.TransformerType.Equals(config.Type, StringComparison.OrdinalIgnoreCase));
            
            if (factory == null)
            {
                Console.Error.WriteLine($"Warning: Unknown transformer type '{config.Type}' in job file. Skipping.");
                continue;
            }
            
            var transformer = factory.CreateFromYamlConfig(config);
            if (transformer != null)
            {
                pipeline.Add(transformer);
            }
        }
        
        return pipeline;
    }


    /// <summary>
    /// Resolves the appropriate factory for a given connection string or file path.
    /// Supports deterministic resolution via "prefix:" and fallback to CanHandle().
    /// Returns the factory and the (potentially cleaned) connection string.
    /// </summary>
    private static (T Factory, string CleanedString) ResolveFactory<T>(IEnumerable<T> factories, string rawString, string typeName) where T : IDataFactory
    {
        // 1. Deterministic Prefix Check
        foreach (var factory in factories)
        {
            var prefix = factory.ProviderName + ":";
            if (rawString.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                // Strip prefix
                var cleaned = rawString.Substring(prefix.Length);
                return (factory, cleaned);
            }
        }

        // 2. Fallback to CanHandle
        var detected = factories.FirstOrDefault(f => f.CanHandle(rawString));
        if (detected != null)
        {
            return (detected, rawString);
        }

        throw new InvalidOperationException($"Could not detect {typeName} provider for '{rawString}'. Please use a known prefix (e.g. 'duckdb:', 'oracle:') or file extension.");
    }

    private static bool IsPrefixMatch(Type optionsType, string configKey)
    {
        // Check "Prefix" static property
        var prefixProp = optionsType.GetProperty("Prefix", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        if (prefixProp != null)
        {
            var prefix = prefixProp.GetValue(null) as string;
            if (string.Equals(prefix, configKey, StringComparison.OrdinalIgnoreCase)) return true;
        }
        return false;
    }
}
