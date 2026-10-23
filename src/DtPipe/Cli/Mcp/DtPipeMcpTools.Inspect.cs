using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Configuration;
using DtPipe.Cli;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Security;
using ModelContextProtocol.Server;
namespace DtPipe.Cli.Mcp;

public partial class DtPipeMcpTools
{

    [McpServerTool(Name = "inspect")]
    [System.ComponentModel.Description("Inspect the schema of a data source. For database sources (e.g. 'sqlite:file.db', 'pg:Host=...'), if no query/table is passed, it automatically discovers and lists available tables and views in the database.")]
    public async Task<string> Inspect(
        [System.ComponentModel.Description("Connection string or file path with provider prefix")] string input,
        [System.ComponentModel.Description("Optional SQL query for database sources")] string? query = null,
        CancellationToken ct = default)
    {
        try
        {
            var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
            var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>().ToList();

            var result = TryResolveReader(registry, readerFactories, input, query);
            if (result == null)
                return JsonSerializer.Serialize(new { error = "No provider found for the given input." });

            bool isTableDiscovery = false;
            if (result.Factory.RequiresQuery && string.IsNullOrWhiteSpace(query))
            {
                var discoveryQuery = TryBuildTableDiscoveryQuery(result.Factory);
                if (discoveryQuery != null)
                {
                    query = discoveryQuery;
                    result = TryResolveReader(registry, readerFactories, input, query);
                    isTableDiscovery = true;
                }
                else
                {
                    return JsonSerializer.Serialize(new { error = $"A query is required for provider '{result.Factory.ComponentName}'." });
                }
            }

            if (result == null)
                return JsonSerializer.Serialize(new { error = "Could not resolve provider with table discovery query." });

            await using var reader = result.Factory.Create(registry);
            await reader.OpenAsync(ct);

            if (reader.Columns == null || reader.Columns.Count == 0)
                return JsonSerializer.Serialize(new { warning = "No columns returned." });

            if (isTableDiscovery)
            {
                var tables = new List<Dictionary<string, object?>>();
                var cols = reader.Columns;
                await foreach (var batch in reader.ReadBatchesAsync(100, ct))
                {
                    var span = batch.Span;
                    for (int i = 0; i < span.Length; i++)
                    {
                        var row = span[i];
                        if (row == null) continue;
                        var dict = new Dictionary<string, object?>();
                        for (int c = 0; c < cols.Count && c < row.Length; c++)
                        {
                            dict[cols[c].Name] = row[c];
                        }
                        tables.Add(dict);
                    }
                }

                return JsonSerializer.Serialize(new
                {
                    info = $"No query or table specified. Automatically discovered available tables/views in database '{result.Factory.ComponentName}':",
                    tables
                }, new JsonSerializerOptions { WriteIndented = true });
            }

            return JsonSerializer.Serialize(
                reader.Columns.Select(c => new
                {
                    c.Name, Type = c.ClrType?.Name ?? "unknown",
                    c.IsNullable
                }),
                new JsonSerializerOptions { WriteIndented = true });
        }
        catch (UnauthorizedAccessException ex)
        {
            return JsonSerializer.Serialize(new { error = ex.Message });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { error = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message) });
        }
    }


    [McpServerTool(Name = "preview-data")]
    [System.ComponentModel.Description("Preview data from a source (up to 10 rows, with automatic masking of sensitive columns). Example input: 'csv:file.csv' or 'pg:Host=localhost;Database=prod;Username=postgres'")]
    public async Task<string> PreviewData(
        [System.ComponentModel.Description("Connection string or file path with provider prefix")] string input,
        [System.ComponentModel.Description("Optional SQL query for database sources")] string? query = null,
        [System.ComponentModel.Description("Number of rows to return (max 10)")] int? limit = 5,
        CancellationToken ct = default)
    {
        try
        {
            var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
            var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>().ToList();

            var result = TryResolveReader(registry, readerFactories, input, query);
            if (result == null)
                return JsonSerializer.Serialize(new { error = "No provider found for the given input." });

            if (result.Factory.RequiresQuery && string.IsNullOrWhiteSpace(query))
                return JsonSerializer.Serialize(new { error = $"A query is required for provider '{result.Factory.ComponentName}'." });

            await using var reader = result.Factory.Create(registry);
            await reader.OpenAsync(ct);

            if (reader.Columns == null || reader.Columns.Count == 0)
                return JsonSerializer.Serialize(new { warning = "No columns returned." });

            int effectiveLimit = Math.Min(limit ?? 5, 10);
            var rowsList = new List<Dictionary<string, object?>>();
            var columns = reader.Columns;

            await foreach (var batch in reader.ReadBatchesAsync(effectiveLimit, ct))
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
        catch (UnauthorizedAccessException ex)
        {
            return JsonSerializer.Serialize(new { error = ex.Message });
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new { error = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(ex.Message) });
        }
    }


    private static string? TryBuildTableDiscoveryQuery(IStreamReaderFactory factory)
    {
        if (factory is IHasSqlDialect hasDialect && hasDialect.Dialect?.TableDiscoveryQuery != null)
        {
            return hasDialect.Dialect.TableDiscoveryQuery;
        }
        return null;
    }

}
