using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli;

/// <summary>
/// Custom help renderer that replaces System.CommandLine help.
/// Collects all flags from components via ICliContributor.GetFlagDefs()
/// and displays them grouped by category.
/// </summary>
public static class HelpRenderer
{
    public static void Print(IServiceProvider sp)
    {
        var console = Console.Error;

        console.WriteLine();
        console.WriteLine("  dtpipe — Data streaming & anonymization CLI");
        console.WriteLine();
        console.WriteLine("USAGE:");
        console.WriteLine("  dtpipe -i <source> [transforms...] -o <target> [options]");
        console.WriteLine("  dtpipe --job <file.yaml>");
        console.WriteLine("  dtpipe <subcommand>");
        console.WriteLine();

        // Subcommands
        console.WriteLine("SUBCOMMANDS:");
        console.WriteLine("  inspect        Inspect a data source schema");
        console.WriteLine("  providers      List available providers");
        console.WriteLine("  completion     Shell completion management");
        console.WriteLine("  secret         Manage keyring secrets");
        console.WriteLine();

        // Structural / global flags from CoreFlagRegistry
        console.WriteLine("GLOBAL OPTIONS:");
        WriteFlag(console, "--input, -i <conn>",      "Input connection string, file path, or '-' for stdin");
        WriteFlag(console, "--output, -o <conn>",     "Output connection string, file path, or '-' for stdout");
        WriteFlag(console, "--from <alias>",          "Read from upstream branch alias (fan-out/tee)");
        WriteFlag(console, "--alias <name>",          "Alias for the current DAG branch");
        WriteFlag(console, "--ref <alias>",           "Secondary source alias for SQL JOINs (preloaded)");
        WriteFlag(console, "--job, -j <file>",        "YAML job file path");
        WriteFlag(console, "--dry-run [N]",           "Dry-run mode (N rows, default: 1)");
        WriteFlag(console, "--no-stats",              "Disable progress bars and statistics");
        WriteFlag(console, "--log <path>",            "Log file path");
        WriteFlag(console, "--export-job <file>",     "Export current CLI as YAML job file");
        WriteFlag(console, "--metrics-path <path>",   "Path to save JSON metrics");
        console.WriteLine();

        // Component-contributed flags, grouped by category
        var readers      = sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>().OfType<ICliContributor>().ToList();
        var writers      = sp.GetRequiredService<IEnumerable<IDataWriterFactory>>().OfType<ICliContributor>().ToList();
        var transformers = sp.GetRequiredService<IEnumerable<IDataTransformerFactory>>().OfType<ICliContributor>().ToList();
        var processors   = sp.GetRequiredService<IEnumerable<IStreamTransformerFactory>>().ToList();

        // Pipeline engine controls
        var pipelineContributor = new PipelineOptionsCliContributor();
        PrintCategory(console, "PIPELINE OPTIONS:", new[] { pipelineContributor });

        // Readers
        PrintCategory(console, "READER OPTIONS:", readers);

        // Transformers
        PrintCategory(console, "TRANSFORMER OPTIONS:", transformers);

        // Processors
        if (processors.Count > 0)
        {
            console.WriteLine("PROCESSOR OPTIONS:");
            foreach (var p in processors)
            {
                foreach (var (flag, isBoolean) in p.CliTriggerFlags)
                {
                    var arity = isBoolean ? "" : " <value>";
                    WriteFlag(console, $"{flag}{arity}", p.ComponentName);
                }
            }
            console.WriteLine();
        }

        // Writers
        PrintCategory(console, "WRITER OPTIONS:", writers);
    }

    private static void PrintCategory(TextWriter console, string header, IEnumerable<ICliContributor> contributors)
    {
        var list = contributors.ToList();
        if (list.Count == 0) return;

        console.WriteLine(header);
        foreach (var contributor in list)
        {
            var flags = contributor.GetFlagDefs().ToList();
            if (flags.Count == 0) continue;

            // Show component name if it's a provider factory
            if (contributor is IDataFactory factory)
            {
                console.WriteLine($"  [{factory.ComponentName}]");
            }

            foreach (var flag in flags)
            {
                var aliases = flag.Aliases.Length > 0 ? $", {string.Join(", ", flag.Aliases)}" : "";
                var arity = flag.Arity switch
                {
                    FlagArity.Boolean    => "",
                    FlagArity.Scalar     => " <value>",
                    FlagArity.Repeatable => " <value...>",
                    _                    => ""
                };
                var desc = flag.Description ?? "";
                WriteFlag(console, $"  {flag.Name}{aliases}{arity}", desc);
            }
        }
        console.WriteLine();
    }

    private static void WriteFlag(TextWriter console, string flag, string description)
    {
        const int padWidth = 30;
        var padded = flag.PadRight(padWidth);
        if (flag.Length >= padWidth)
            console.WriteLine($"  {padded}  {description}");
        else
            console.WriteLine($"  {padded}{description}");
    }
}
