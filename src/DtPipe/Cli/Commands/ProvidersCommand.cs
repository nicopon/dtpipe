using System;
using System.Linq;
using System.Threading.Tasks;
using System.CommandLine;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

public class ProvidersCommand : Command
{
    private readonly IAnsiConsole _console;

    public ProvidersCommand(IServiceProvider serviceProvider)
        : base("providers", "List all available data providers")
    {
        _console = serviceProvider.GetRequiredService<IAnsiConsole>();

        var formatOption = new Option<string>("--format") { Description = "Output format (table, json)" };
        formatOption.DefaultValueFactory = _ => "table";
        formatOption.AcceptOnlyFromAmong("table", "json");
        Options.Add(formatOption);

        this.SetAction((parseResult, ct) =>
        {
            var readers = serviceProvider.GetServices<IStreamReaderFactory>();
            var writers = serviceProvider.GetServices<IDataWriterFactory>();
            var streamTransformers = serviceProvider.GetServices<IStreamTransformerFactory>();

            if (string.Equals(parseResult.GetValue(formatOption), "json", StringComparison.OrdinalIgnoreCase))
            {
                // The inventory is this command's primary data, so it goes to stdout like
                // `inspect --format json`; the table keeps the console.
                var entries = readers.Select(r => Describe(r, "reader"))
                    .Concat(writers.Select(w => Describe(w, "writer")))
                    .Concat(streamTransformers.Select(st => new ProviderEntry(st.ComponentName, "stream-processor", false, st.Category)))
                    .OrderBy(e => e.Name, StringComparer.Ordinal).ThenBy(e => e.Kind, StringComparer.Ordinal);
                Console.Out.WriteLine(System.Text.Json.JsonSerializer.Serialize(entries, JsonOptions));
                return Task.CompletedTask;
            }

            var table = new Table()
                .AddColumn("Provider")
                .AddColumn("Type")
                .AddColumn("Stdio")
                .AddColumn("Category");

            foreach (var r in readers.OrderBy(r => r.ComponentName))
            {
                bool supportsStdio = false;
                string category = "Reader";
                if (r is IDataFactory df)
                {
                    supportsStdio = df.SupportsStdio;
                    category = df.Category;
                }

                table.AddRow(
                    $"[cyan]{Markup.Escape(r.ComponentName)}[/]",
                    "[green]Reader[/]",
                    supportsStdio ? "✓" : "",
                    Markup.Escape(category));
            }

            foreach (var w in writers.OrderBy(w => w.ComponentName))
            {
                bool supportsStdio = false;
                string category = "Writer";
                if (w is IDataFactory df)
                {
                    supportsStdio = df.SupportsStdio;
                    category = df.Category;
                }

                table.AddRow(
                    $"[yellow]{Markup.Escape(w.ComponentName)}[/]",
                    "[blue]Writer[/]",
                    supportsStdio ? "✓" : "",
                    Markup.Escape(category));
            }

            foreach (var st in streamTransformers.OrderBy(st => st.ComponentName))
            {
                table.AddRow(
                    $"[magenta]{Markup.Escape(st.ComponentName)}[/]",
                    "[magenta]Stream Processor[/]",
                    "",
                    Markup.Escape(st.Category));
            }

            _console.MarkupLine("[bold]📦 Registered Providers[/]");
            _console.WriteLine();
            _console.Write(table);

            return Task.CompletedTask;
        });
    }

    private static readonly System.Text.Json.JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase,
    };

    private sealed record ProviderEntry(string Name, string Kind, bool Stdio, string Category);

    private static ProviderEntry Describe(IDataFactory factory, string kind)
        => new(factory.ComponentName, kind, factory.SupportsStdio, factory.Category);
}
