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

        this.SetAction((parseResult, ct) =>
        {
            var readers = serviceProvider.GetServices<IStreamReaderFactory>();
            var writers = serviceProvider.GetServices<IDataWriterFactory>();
            var streamTransformers = serviceProvider.GetServices<IStreamTransformerFactory>();

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
}
