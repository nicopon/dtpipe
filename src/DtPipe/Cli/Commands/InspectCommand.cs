using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

public class InspectCommand : Command
{
    public InspectCommand(IServiceProvider serviceProvider) : base("inspect", "Inspect the schema of a data source")
    {
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
                var console = serviceProvider.GetRequiredService<IAnsiConsole>();
                console.MarkupLine("[red]Error:[/] --input is required.");
                return;
            }

            var query = parseResult.GetValue(queryOption);
            var format = parseResult.GetValue(formatOption) ?? "table";

            await ExecuteAsync(serviceProvider, input, query, format, ct);
        });
    }

    private static async Task ExecuteAsync(
        IServiceProvider sp,
        string input,
        string? query,
        string format,
        CancellationToken ct)
    {
        var console = sp.GetRequiredService<IAnsiConsole>();
        var registry = sp.GetRequiredService<OptionsRegistry>();
        var readerFactories = sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>().ToList();

        // 1. Résoudre le reader via le même mécanisme que l'export
        //    (parser le prefix, trouver la factory via ComponentName ou CanHandle)
        IStreamReaderFactory? factory = null;
        foreach (var f in readerFactories)
        {
            if (input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase))
            {
                // Strip prefix, set connection string in registry
                var connStr = input.Substring(f.ComponentName.Length + 1);

                var optionsType = f.GetSupportedOptionTypes().FirstOrDefault();
                if (optionsType != null)
                {
                    var instance = registry.Get(optionsType);
                    optionsType.GetProperty("Input")?.SetValue(instance, connStr);
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

        // 2. Set query if provided
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
