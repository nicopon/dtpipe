using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

public class InspectCommand : Command
{
    private readonly IAnsiConsole _console;

    public InspectCommand(IServiceProvider serviceProvider) : base("inspect", "Inspect the schema of a data source")
    {
        _console = serviceProvider.GetRequiredService<IAnsiConsole>();

        var inputOption = new Option<string>("--input") { Description = "Connection string or file path" };
        inputOption.Aliases.Add("-i");

        var queryOption = new Option<string?>("--query") { Description = "SQL query (for database sources)" };
        queryOption.Aliases.Add("-q");

        var formatOption = new Option<string>("--format") { Description = "Output format (table, json)" };
        formatOption.DefaultValueFactory = _ => "table";

        Options.Add(inputOption);
        Options.Add(queryOption);
        Options.Add(formatOption);

        this.SetAction(async (parseResult, ct) =>
        {
            var input = parseResult.GetValue(inputOption);
            if (string.IsNullOrWhiteSpace(input))
            {
                _console.MarkupLine("[red]Error:[/] --input is required.");
                return;
            }

            var query = parseResult.GetValue(queryOption);
            var format = parseResult.GetValue(formatOption) ?? "table";

            await ExecuteAsync(serviceProvider, _console, input, query, format, ct);
        });
    }

    private static async Task ExecuteAsync(
        IServiceProvider sp,
        IAnsiConsole console,
        string input,
        string? query,
        string format,
        CancellationToken ct)
    {
        var registry = sp.GetRequiredService<OptionsRegistry>();
        var readerFactories = sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>().ToList();

        // 1. Resolve the reader using the same mechanism as export
        //    (parse the prefix, find the factory via ComponentName or CanHandle)
        string effectiveConnectionString = input;
        IStreamReaderFactory? factory = null;
        foreach (var f in readerFactories)
        {
            if (input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase))
            {
                // Strip prefix, set connection string
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
            // Fallback: try CanHandle
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
        {
            console.MarkupLine("[red]Error:[/] No provider found for the given input.");
            return;
        }

        // Register the connection string into PipelineOptions so the factory can pick it up
        var pipelineOpts = registry.Get<PipelineOptions>();
        var updatedPipelineOpts = pipelineOpts with { ConnectionString = effectiveConnectionString, Query = query };
        registry.RegisterByType(typeof(PipelineOptions), updatedPipelineOpts);

        // 2. Set query if provided
        if (factory.RequiresQuery && string.IsNullOrWhiteSpace(query))
        {
            console.MarkupLine($"[red]Error:[/] A query is required for provider '{factory.ComponentName}'. Use --query \"SELECT...\"");
            return;
        }

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

        // 3. Open reader and get columns
        await using var reader = factory.Create(registry);
        await reader.OpenAsync(ct);

        if (reader.Columns == null || reader.Columns.Count == 0)
        {
            console.MarkupLine("[yellow]⚠ No columns returned.[/]");
            return;
        }

        // 4. Render output
        if (format.Equals("json", StringComparison.OrdinalIgnoreCase))
        {
            // Output JSON array of column descriptors
            var json = System.Text.Json.JsonSerializer.Serialize(
                reader.Columns.Select(c => new {
                    c.Name, Type = c.ClrType?.Name ?? "unknown",
                    c.IsNullable
                }),
                new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
            
            // Note: JSON output from 'inspect' is considered Primary Data for THIS command,
            // so we keep it on STDOUT. But for technical consistency, one might argue it should go to console.Error
            // if we want to pipe STDOUT to another dtpipe. However, 'inspect' isn't usually piped to 'dtpipe'.
            Console.WriteLine(json);
        }
        else
        {
            // Spectre.Console table
            var table = new Table()
                .AddColumn("#")
                .AddColumn("Column")
                .AddColumn("Type")
                .AddColumn("Nullable");

            for (int i = 0; i < reader.Columns.Count; i++)
            {
                var col = reader.Columns[i];
                table.AddRow(
                    (i + 1).ToString(),
                    Markup.Escape(col.Name),
                    col.ClrType?.Name ?? "[dim]unknown[/]",
                    col.IsNullable ? "[green]✓[/]" : "[red]✗[/]");
            }

            console.MarkupLine($"[bold]📋 Schema for[/] [cyan]{Markup.Escape(input)}[/]");
            console.MarkupLine($"[dim]{reader.Columns.Count} columns[/]");
            console.WriteLine();
            console.Write(table);
        }
    }
}
