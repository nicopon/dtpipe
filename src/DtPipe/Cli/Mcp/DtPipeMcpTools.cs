using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Cli.Pipeline;
using ModelContextProtocol.Server;

namespace DtPipe.Cli.Mcp;

public class DtPipeMcpTools
{
    private readonly IEnumerable<IStreamReaderFactory> _readerFactories;
    private readonly IEnumerable<IDataTransformerFactory> _transformerFactories;
    private readonly IEnumerable<IDataWriterFactory> _writerFactories;
    private readonly IServiceProvider _serviceProvider;

    public DtPipeMcpTools(
        IEnumerable<IStreamReaderFactory> readerFactories,
        IEnumerable<IDataTransformerFactory> transformerFactories,
        IEnumerable<IDataWriterFactory> writerFactories,
        IServiceProvider serviceProvider)
    {
        _readerFactories = readerFactories;
        _transformerFactories = transformerFactories;
        _writerFactories = writerFactories;
        _serviceProvider = serviceProvider;
    }

    [McpServerTool(Name = "list-providers")]
    [System.ComponentModel.Description("List available data source providers, writers, and transformers in dtpipe")]
    public string ListProviders()
    {
        var readers = _readerFactories.Select(f => f.ComponentName).ToList();
        var transformers = _transformerFactories.Select(f => f.ComponentName).ToList();
        var writers = _writerFactories.Select(f => f.ComponentName).ToList();
        
        return JsonSerializer.Serialize(new 
        { 
            readers, 
            transformers, 
            writers 
        }, new JsonSerializerOptions { WriteIndented = true });
    }

    [McpServerTool(Name = "inspect")]
    [System.ComponentModel.Description("Inspect the schema of a data source. Example input: 'pg:Host=localhost;Database=prod;Username=postgres' or 'csv:file.csv'")]
    public async Task<string> Inspect(
        [System.ComponentModel.Description("Connection string or file path with provider prefix")] string input, 
        [System.ComponentModel.Description("Optional SQL query for database sources")] string? query = null)
    {
        var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
        var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>().ToList();

        string effectiveConnectionString = input;
        IStreamReaderFactory? factory = null;
        
        foreach (var f in readerFactories)
        {
            if (input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase))
            {
                effectiveConnectionString = input.Substring(f.ComponentName.Length + 1);
                var optionsType = f.GetSupportedOptionTypes().FirstOrDefault();
                if (optionsType != null)
                {
                    var instance = registry.Get(optionsType);
                    optionsType.GetProperty("Input")?.SetValue(instance, effectiveConnectionString);
                    registry.RegisterByType(optionsType, instance);
                }
                factory = f;
                break;
            }
        }
        
        if (factory == null)
        {
            foreach (var f in readerFactories)
            {
                if (f.CanHandle(input))
                {
                    var optionsType = f.GetSupportedOptionTypes().FirstOrDefault();
                    if (optionsType != null)
                    {
                        var instance = registry.Get(optionsType);
                        optionsType.GetProperty("Input")?.SetValue(instance, input);
                        registry.RegisterByType(optionsType, instance);
                    }
                    factory = f;
                    break;
                }
            }
        }
        
        if (factory == null)
            return JsonSerializer.Serialize(new { error = "No provider found for the given input." });

        // Perform path safety validation
        try
        {
            ValidatePathSafety(effectiveConnectionString);
        }
        catch (UnauthorizedAccessException ex)
        {
            return JsonSerializer.Serialize(new { error = ex.Message });
        }

        registry.Register(new DtPipe.Cli.Infrastructure.ConnectionRoute(effectiveConnectionString, string.Empty));
        var readerOpts = registry.Get(factory.OptionsType) as DtPipe.Core.Options.IQueryAwareOptions;
        if (readerOpts != null && !string.IsNullOrWhiteSpace(query))
            readerOpts.Query = query;

        if (factory.RequiresQuery && string.IsNullOrWhiteSpace(query))
            return JsonSerializer.Serialize(new { error = $"A query is required for provider '{factory.ComponentName}'." });

        if (!string.IsNullOrEmpty(query))
        {
            var optionsType = factory.GetSupportedOptionTypes().FirstOrDefault();
            if (optionsType != null)
            {
                var instance = registry.Get(optionsType);
                optionsType.GetProperty("Query")?.SetValue(instance, query);
                registry.RegisterByType(optionsType, instance);
            }
        }

        try
        {
            await using var reader = factory.Create(registry);
            await reader.OpenAsync(CancellationToken.None);

            if (reader.Columns == null || reader.Columns.Count == 0)
                return JsonSerializer.Serialize(new { warning = "No columns returned." });

            return JsonSerializer.Serialize(
                reader.Columns.Select(c => new {
                    c.Name, Type = c.ClrType?.Name ?? "unknown",
                    c.IsNullable
                }),
                new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { error = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message) });
        }
    }

    [McpServerTool(Name = "validate-pipeline")]
    [System.ComponentModel.Description("Validate a dtpipe command line or job configuration. E.g. 'dtpipe -i source.csv -o target.parquet' or 'dtpipe --job config.yaml'")]
    public string ValidatePipeline(
        [System.ComponentModel.Description("The full dtpipe command line string to validate")] string command)
    {
        try
        {
            // Clean up command prefix if present
            string cmdLine = command.Trim();
            if (cmdLine.StartsWith("dtpipe ", StringComparison.OrdinalIgnoreCase))
            {
                cmdLine = cmdLine.Substring(7).Trim();
            }

            var args = SplitArguments(cmdLine);

            var registry = FlagRegistryFactory.Build(_serviceProvider);
            var streamTransformerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();

            var lexer = new PipelineLexer(registry);
            var parsedPipeline = lexer.Parse(args);
            
            var secretsManager = _serviceProvider.GetRequiredService<DtPipe.Cli.Security.ISecretsManager>();
            var (jobs, dag, _) = PipelineToJobConverter.Convert(parsedPipeline, streamTransformerFactories, secretsManager);

            var errors = PipelineValidator.Validate(dag, jobs, streamTransformerFactories);

            if (errors.Count > 0)
            {
                return JsonSerializer.Serialize(new
                {
                    success = false,
                    errors
                }, new JsonSerializerOptions { WriteIndented = true });
            }

            // Return success with DAG metadata
            var branches = dag.Branches.Select(b => new
            {
                b.Alias,
                StreamingFrom = b.StreamingAliases,
                Referencing = b.RefAliases,
                Input = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(b.Input),
                Output = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(b.Output),
                Processor = b.ProcessorName ?? "none",
                TransformersCount = jobs.TryGetValue(b.Alias, out var j) ? j.Transformers?.Count ?? 0 : 0
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                success = true,
                message = "Pipeline syntax and topology are valid.",
                branches
            }, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new
            {
                success = false,
                errors = new[] { DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message) }
            }, new JsonSerializerOptions { WriteIndented = true });
        }
    }

    [McpServerTool(Name = "preview-data")]
    [System.ComponentModel.Description("Preview data from a source (up to 10 rows, with automatic masking of sensitive columns). Example input: 'csv:file.csv' or 'pg:Host=localhost;Database=prod;Username=postgres'")]
    public async Task<string> PreviewData(
        [System.ComponentModel.Description("Connection string or file path with provider prefix")] string input,
        [System.ComponentModel.Description("Optional SQL query for database sources")] string? query = null,
        [System.ComponentModel.Description("Number of rows to return (max 10)")] int? limit = 5)
    {
        var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
        var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>().ToList();

        string effectiveConnectionString = input;
        IStreamReaderFactory? factory = null;

        foreach (var f in readerFactories)
        {
            if (input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase))
            {
                effectiveConnectionString = input.Substring(f.ComponentName.Length + 1);
                var optionsType = f.GetSupportedOptionTypes().FirstOrDefault();
                if (optionsType != null)
                {
                    var instance = registry.Get(optionsType);
                    optionsType.GetProperty("Input")?.SetValue(instance, effectiveConnectionString);
                    registry.RegisterByType(optionsType, instance);
                }
                factory = f;
                break;
            }
        }

        if (factory == null)
        {
            foreach (var f in readerFactories)
            {
                if (f.CanHandle(input))
                {
                    var optionsType = f.GetSupportedOptionTypes().FirstOrDefault();
                    if (optionsType != null)
                    {
                        var instance = registry.Get(optionsType);
                        optionsType.GetProperty("Input")?.SetValue(instance, input);
                        registry.RegisterByType(optionsType, instance);
                    }
                    factory = f;
                    break;
                }
            }
        }

        if (factory == null)
            return JsonSerializer.Serialize(new { error = "No provider found for the given input." });

        // Perform path safety validation
        try
        {
            ValidatePathSafety(effectiveConnectionString);
        }
        catch (UnauthorizedAccessException ex)
        {
            return JsonSerializer.Serialize(new { error = ex.Message });
        }

        registry.Register(new DtPipe.Cli.Infrastructure.ConnectionRoute(effectiveConnectionString, string.Empty));
        var readerOpts = registry.Get(factory.OptionsType) as DtPipe.Core.Options.IQueryAwareOptions;
        if (readerOpts != null && !string.IsNullOrWhiteSpace(query))
            readerOpts.Query = query;

        if (factory.RequiresQuery && string.IsNullOrWhiteSpace(query))
            return JsonSerializer.Serialize(new { error = $"A query is required for provider '{factory.ComponentName}'." });

        if (!string.IsNullOrEmpty(query))
        {
            var optionsType = factory.GetSupportedOptionTypes().FirstOrDefault();
            if (optionsType != null)
            {
                var instance = registry.Get(optionsType);
                optionsType.GetProperty("Query")?.SetValue(instance, query);
                registry.RegisterByType(optionsType, instance);
            }
        }

        try
        {
            await using var reader = factory.Create(registry);
            await reader.OpenAsync(CancellationToken.None);

            if (reader.Columns == null || reader.Columns.Count == 0)
                return JsonSerializer.Serialize(new { warning = "No columns returned." });

            int effectiveLimit = Math.Min(limit ?? 5, 10);
            var rowsList = new List<Dictionary<string, object?>>();
            var columns = reader.Columns;

            await foreach (var batch in reader.ReadBatchesAsync(effectiveLimit, CancellationToken.None))
            {
                var span = batch.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    var row = span[i];
                    if (row == null) continue;

                    var rowDict = new Dictionary<string, object?>();
                    for (int c = 0; c < columns.Count && c < row.Length; c++)
                    {
                        var colName = columns[c].Name;
                        rowDict[colName] = row[c];
                    }
                    rowsList.Add(rowDict);

                    if (rowsList.Count >= effectiveLimit)
                        break;
                }

                if (rowsList.Count >= effectiveLimit)
                    break;
            }

            return JsonSerializer.Serialize(rowsList, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { error = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message) });
        }
    }

    private static void ValidatePathSafety(string path)
    {
        if (string.IsNullOrWhiteSpace(path)) return;

        // Clean query/parameters from SQLite/DuckDB connection strings
        string cleanPath = path;
        int semicolonIndex = cleanPath.IndexOf(';');
        if (semicolonIndex >= 0)
        {
            // If it's a connection string containing key=value pairs, skip file path check
            if (cleanPath.Contains("Host=", StringComparison.OrdinalIgnoreCase) ||
                cleanPath.Contains("Server=", StringComparison.OrdinalIgnoreCase) ||
                cleanPath.Contains("User Id=", StringComparison.OrdinalIgnoreCase) ||
                cleanPath.Contains("Database=", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }
            // Otherwise, it might be an SQLite/DuckDB file connection string like "Data Source=filename;..."
            // Extract the path if it contains "Data Source="
            var match = Regex.Match(cleanPath, @"Data\s+Source\s*=\s*(?<file>[^;]+)", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                cleanPath = match.Groups["file"].Value.Trim();
            }
            else
            {
                cleanPath = cleanPath.Substring(0, semicolonIndex).Trim();
            }
        }

        // Strip quotes if any
        cleanPath = cleanPath.Trim('"', '\'');

        // Skip check if it is clearly in-memory
        if (string.Equals(cleanPath, ":memory:", StringComparison.OrdinalIgnoreCase) || string.Equals(cleanPath, "-", StringComparison.Ordinal))
        {
            return;
        }

        try
        {
            string fullPath = Path.GetFullPath(cleanPath);
            string currentDir = Path.GetFullPath(Directory.GetCurrentDirectory());

            if (!fullPath.StartsWith(currentDir, StringComparison.OrdinalIgnoreCase))
            {
                throw new UnauthorizedAccessException($"Access to path '{path}' is denied. Only files within the current workspace are accessible.");
            }
        }
        catch (UnauthorizedAccessException)
        {
            throw;
        }
        catch (Exception)
        {
            // If Path.GetFullPath throws, it might be a database connection string with invalid characters.
            // We ignore it here and let the provider handle/reject it.
        }
    }


    private static string[] SplitArguments(string commandLine)
    {
        var args = new List<string>();
        var current = new System.Text.StringBuilder();
        bool inQuotes = false;
        
        for (int i = 0; i < commandLine.Length; i++)
        {
            char c = commandLine[i];
            if (c == '"')
            {
                inQuotes = !inQuotes;
            }
            else if (char.IsWhiteSpace(c) && !inQuotes)
            {
                if (current.Length > 0)
                {
                    args.Add(current.ToString());
                    current.Clear();
                }
            }
            else
            {
                current.Append(c);
            }
        }
        
        if (current.Length > 0)
        {
            args.Add(current.ToString());
        }
        
        return args.ToArray();
    }
}
