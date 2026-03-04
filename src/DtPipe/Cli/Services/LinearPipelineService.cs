using System.CommandLine;
using DtPipe.Cli.Infrastructure;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Validation;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Services;

/// <summary>
/// Responsible for orchestrating linear (single-branch) pipeline execution.
/// </summary>
public class LinearPipelineService
{
    private readonly IEnumerable<ICliContributor> _contributors;
    private readonly IServiceProvider _serviceProvider;
    private readonly OptionsRegistry _registry;
    private readonly IAnsiConsole _console;

    public LinearPipelineService(
        IEnumerable<ICliContributor> contributors,
        IServiceProvider serviceProvider,
        OptionsRegistry registry,
        IAnsiConsole console)
    {
        _contributors = contributors;
        _serviceProvider = serviceProvider;
        _registry = registry;
        _console = console;
    }

    public async Task<int> ExecuteAsync(
        JobDefinition job,
        string[] currentRawArgs,
        CancellationToken token,
        string? localAlias,
        bool isDag)
    {
        if (isDag) _console.MarkupLine($"[grey]DEBUG: LinearPipelineService.ExecuteAsync started for alias '{localAlias}'[/]");
        var exportService = _serviceProvider.GetRequiredService<ExportService>();

        var readerFactories = _contributors.OfType<IStreamReaderFactory>().ToList();
        var (readerFactory, cleanedInput) = ResolveFactory(readerFactories, job.Input, "reader");
        job = job with { Input = cleanedInput };

        if (!isDag) _console.WriteLine($"Auto-detected input source: {readerFactory.ComponentName}");

        var writerFactories = _contributors.OfType<IDataWriterFactory>().ToList();
        var (writerFactory, cleanedOutput) = ResolveFactory(writerFactories, job.Output, "writer");
        job = job with { Output = cleanedOutput };

        job = job with
        {
            Query = LoadOrReadContent(job.Query, _console, "query"),
            PreExec = LoadOrReadContent(job.PreExec, _console, "Pre-Exec"),
            PostExec = LoadOrReadContent(job.PostExec, _console, "Post-Exec"),
            OnErrorExec = LoadOrReadContent(job.OnErrorExec, _console, "On-Error-Exec"),
            FinallyExec = LoadOrReadContent(job.FinallyExec, _console, "Finally-Exec")
        };

        if (readerFactory.RequiresQuery)
        {
            if (string.IsNullOrWhiteSpace(job.Query))
            {
                _console.Write(new Spectre.Console.Markup($"[red]Error: A query is required for provider '{readerFactory.ComponentName}'. Use --query \"SELECT...\"[/]{Environment.NewLine}"));
                return 1;
            }
            try { SqlQueryValidator.Validate(job.Query, job.UnsafeQuery); }
            catch (InvalidOperationException ex)
            {
                _console.Write(new Spectre.Console.Markup($"[red]Error: {ex.Message}[/]{Environment.NewLine}"));
                return 1;
            }
        }

        var pipelineOptions = new PipelineOptions
        {
            Provider = readerFactory.ComponentName,
            ConnectionString = job.Input,
            Query = job.Query,
            OutputPath = job.Output ?? "",
            ConnectionTimeout = job.ConnectionTimeout,
            QueryTimeout = job.QueryTimeout,
            BatchSize = job.BatchSize,
            UnsafeQuery = job.UnsafeQuery,
            DryRunCount = RawJobBuilder.ParseDryRunFromArgs(currentRawArgs),
            Limit = job.Limit,
            SamplingRate = job.SamplingRate,
            SamplingSeed = job.SamplingSeed,
            LogPath = job.LogPath,
            Key = job.Key,
            PreExec = job.PreExec,
            PostExec = job.PostExec,
            OnErrorExec = job.OnErrorExec,
            FinallyExec = job.FinallyExec,
            Strategy = job.Strategy,
            InsertMode = job.InsertMode,
            Table = job.Table,
            MaxRetries = job.MaxRetries,
            RetryDelayMs = job.RetryDelayMs,
            StrictSchema = job.StrictSchema,
            NoSchemaValidation = job.NoSchemaValidation,
            NoStats = job.NoStats,
            MetricsPath = job.MetricsPath,
            AutoMigrate = job.AutoMigrate ?? false
        };

        _registry.Register(pipelineOptions);

        var tFactories = _contributors.OfType<IDataTransformerFactory>().ToList();
        List<IDataTransformer> pipeline;

        if (job.Transformers != null && job.Transformers.Count > 0)
        {
            pipeline = BuildPipelineFromYaml(job.Transformers, tFactories, _console);
        }
        else
        {
            var pipelineBuilder = new TransformerPipelineBuilder(tFactories);
            pipeline = pipelineBuilder.Build(currentRawArgs);
        }

        try
        {
            if (isDag) _console.MarkupLine($"[grey]DEBUG: Calling RunExportAsync for alias '{localAlias}'[/]");
            await exportService.RunExportAsync(pipelineOptions, pipelineOptions.Provider, pipelineOptions.OutputPath, token, pipeline, readerFactory, writerFactory, _registry, localAlias);
            if (isDag) _console.MarkupLine($"[grey]DEBUG: RunExportAsync completed for alias '{localAlias}'[/]");
            return 0;
        }
        catch (Exception ex)
        {
            _console.Write(new Spectre.Console.Markup($"{Environment.NewLine}[red]Error: {Markup.Escape(ex.Message)}[/]{Environment.NewLine}"));
            if (pipelineOptions.Provider == "duckdb" || Environment.GetEnvironmentVariable("DEBUG") == "1")
                _console.WriteLine(ex.StackTrace ?? "");
            return 1;
        }
    }

    private static (T Factory, string CleanedString) ResolveFactory<T>(IEnumerable<T> factories, string rawString, string typeName) where T : IDataFactory
    {
        rawString = rawString.Trim();

        // 1. Deterministic Prefix Check
        foreach (var factory in factories)
        {
            var prefix = factory.ComponentName + ":";
            if (rawString.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                var cleaned = rawString.Substring(prefix.Length).Trim();
                if (cleaned == "-" && !factory.SupportsStdio)
                {
                    throw new InvalidOperationException($"The provider '{factory.ComponentName}' does not support standard input/output pipes (-).");
                }
                return (factory, cleaned);
            }

            if (rawString.Equals(factory.ComponentName, StringComparison.OrdinalIgnoreCase))
            {
                // If the raw string is just the component name, it implies using STDIN/OUT (-).
                if (!factory.SupportsStdio)
                {
                    throw new InvalidOperationException($"The provider '{factory.ComponentName}' does not support standard input/output pipes (-).");
                }
                return (factory, "-");
            }
        }

        // 2. Fallback to CanHandle() rules
        var matchingFactories = factories.Where(f => f.CanHandle(rawString)).ToList();
        if (matchingFactories.Count == 1)
        {
            var factory = matchingFactories[0];
            if (rawString == "-" && !factory.SupportsStdio)
            {
                throw new InvalidOperationException($"The provider '{factory.ComponentName}' does not support standard input/output pipes (-).");
            }
            return (factory, rawString);
        }

        if (matchingFactories.Count > 1)
        {
            var names = string.Join(", ", matchingFactories.Select(f => f.ComponentName));
            throw new InvalidOperationException($"Ambiguous {typeName} string '{rawString}'. Matches: {names}. Use explicit prefix (e.g. '{matchingFactories[0].ComponentName}:...') to disambiguate.");
        }

        throw new InvalidOperationException($"No compatible {typeName} found for connection string '{rawString}'.");
    }

    private static string? LoadOrReadContent(string? input, Spectre.Console.IAnsiConsole console, string label)
    {
        if (string.IsNullOrWhiteSpace(input)) return input;
        input = input.Trim();

        string? filename = null;
        if (input.StartsWith("@"))
        {
            filename = input.Substring(1).Trim();
        }
        else if (File.Exists(input) && (input.EndsWith(".sql", StringComparison.OrdinalIgnoreCase) || input.EndsWith(".txt", StringComparison.OrdinalIgnoreCase) || input.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) || input.EndsWith(".yml", StringComparison.OrdinalIgnoreCase)))
        {
            // Auto-detect file path if it exists and has a common extension
            filename = input;
        }

        if (filename != null)
        {
            if (File.Exists(filename))
            {
                console.WriteLine($"Loaded {label} from File: {filename}");
                return File.ReadAllText(filename).Trim();
            }
            else if (input.StartsWith("@"))
            {
                throw new InvalidOperationException($"File not found for {label}: {filename}");
            }
        }
        return input;
    }

    private static List<IDataTransformer> BuildPipelineFromYaml(
        List<TransformerConfig> configs,
        List<IDataTransformerFactory> factories,
        IAnsiConsole console)
    {
        var pipeline = new List<IDataTransformer>();
        foreach (var config in configs)
        {
            var factory = factories.FirstOrDefault(f =>
                f.ComponentName.Equals(config.Type, StringComparison.OrdinalIgnoreCase));

            if (factory == null)
            {
                console.Write(new Markup($"[yellow]Warning: Unknown transformer type '{config.Type}' in job file. Skipping.[/]{Environment.NewLine}"));
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
}
