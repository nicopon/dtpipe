using System;
using System.Linq;
using System.Threading.Tasks;
using System.CommandLine;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Cli.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

public class ProvidersCommand : Command
{
    public ProvidersCommand(IServiceProvider serviceProvider)
        : base("providers", "List all available data providers")
    {
        this.SetAction((parseResult, ct) =>
        {
            var console = serviceProvider.GetRequiredService<IAnsiConsole>();
            var readers = serviceProvider.GetServices<IStreamReaderFactory>();
            var writers = serviceProvider.GetServices<IDataWriterFactory>();
            var xstreamers = serviceProvider.GetServices<IXStreamerFactory>();

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

            foreach (var x in xstreamers.OrderBy(x => x.ComponentName))
            {
                bool supportsStdio = false;
                string category = "XStreamer";
                if (x is IDataFactory df)
                {
                    supportsStdio = df.SupportsStdio;
                    category = df.Category;
                }

                table.AddRow(
                    $"[magenta]{Markup.Escape(x.ComponentName)}[/]",
                    "[magenta]XStreamer[/]",
                    supportsStdio ? "✓" : "",
                    Markup.Escape(category));
            }

            console.MarkupLine("[bold]📦 Registered Providers[/]");
            console.WriteLine();
            console.Write(table);

            return Task.CompletedTask;
        });
    }
}
