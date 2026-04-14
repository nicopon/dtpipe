using System.CommandLine;
using DtPipe.Processors.Sql;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

/// <summary>
/// Lists SQL engines compiled into this distribution and their availability status.
///
/// Usage:
///   dtpipe sql-engines                 Human-readable table
///   dtpipe sql-engines datafusion      Exit 0 if available, exit 1 if not — no output (for scripting)
///   dtpipe sql-engines duckdb          Same for any engine name
/// </summary>
public class SqlEnginesCommand : Command
{
    public SqlEnginesCommand(IServiceProvider serviceProvider)
        : base("sql-engines", "List available SQL engines and their capabilities")
    {
        // Optional engine name — when provided, exits 0/1 with no output (scripting mode).
        var engineArg = new Argument<string?>("engine")
        {
            Description = "Engine name to check. Exits 0 if available, 1 if not — no output (for use in scripts).",
            Arity = ArgumentArity.ZeroOrOne,
            DefaultValueFactory = _ => null
        };
        Arguments.Add(engineArg);

        var console = serviceProvider.GetRequiredService<IAnsiConsole>();

        this.SetAction((parseResult, ct) =>
        {
            var engines = CompositeSqlTransformerFactory.GetEngines();
            var engineName = parseResult.GetValue(engineArg);

            if (engineName is not null)
            {
                // Scripting mode: no output, exit code only.
                var found = engines.FirstOrDefault(
                    e => e.Name.Equals(engineName, StringComparison.OrdinalIgnoreCase));
                Environment.ExitCode = found?.Available == true ? 0 : 1;
                return Task.CompletedTask;
            }

            // Human-readable table.
            var table = new Table()
                .AddColumn("Engine")
                .AddColumn("Status")
                .AddColumn("Default")
                .AddColumn("Notes");

            foreach (var e in engines)
            {
                var status = e.Available ? "[green]available[/]" : "[grey]unavailable[/]";
                var def = e.IsDefault ? "[cyan]✓ default[/]" : "";
                var name = e.Available
                    ? $"[bold]{Markup.Escape(e.Name)}[/]"
                    : $"[grey]{Markup.Escape(e.Name)}[/]";

                table.AddRow(name, status, def, Markup.Escape(e.Description));
            }

            console.MarkupLine("[bold]⚡ SQL Engines[/]");
            console.WriteLine();
            console.Write(table);
            console.WriteLine();
            console.MarkupLine("[grey]Use [bold]--sql-engine <name>[/] or [bold]DTPIPE_SQL_ENGINE=<name>[/] to select an engine.[/]");
            console.MarkupLine("[grey]Use [bold]dtpipe sql-engines <name>[/] in scripts to check availability (exit 0/1).[/]");

            return Task.CompletedTask;
        });
    }
}
