using System.CommandLine;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Split;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;
using DtPipe.DryRun;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

/// <summary>
/// Offers the points at which a job could be cut in two, and what would cross at each.
///
/// <para>
/// The candidates are read off a <b>sample run</b> of the job itself — the real reader, the real
/// transformers, the writer neutralised — through the tap the engine already offers each stage.
/// There is no analyser beside <c>PipelineExecutor</c> (CLAUDE.md › "Sample mode — there is no
/// second engine"), and one that walked the transformer list to guess what each would emit would
/// be <c>DryRunAnalyzer</c> again: it reported a windowed pipeline as dropping every row, because
/// what a transformer emits is not derivable from what it is.
/// </para>
///
/// <para>
/// It proposes and never decides. Where to cut depends on who filters, who transforms and who pays
/// for each task, none of which is in the job file; the cost column is information beside the list,
/// not a ranking.
/// </para>
/// </summary>
public class SplitCommand : Command
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IAnsiConsole _console;

    public SplitCommand(IServiceProvider serviceProvider, IAnsiConsole console)
        : base("split", "Offer the points at which a job could be cut in two")
    {
        _serviceProvider = serviceProvider;
        _console = console;

        var jobArgument = new Argument<string>("job")
        {
            Description = "The job file to cut. Produce one from a command line with --export-job",
        };
        var branchOption = new Option<string?>("--branch")
        {
            Description = "Which branch to cut, when the job has more than one reading a source",
        };
        var rowsOption = new Option<int>("--rows")
        {
            Description = "How many source rows the sample reads (default 10)",
            DefaultValueFactory = _ => 10,
        };
        var atOption = new Option<int?>("--at")
        {
            Description = "Cut after this stage instead of listing the candidates",
        };
        var outOption = new Option<string?>("--out")
        {
            Description = "Where to write the halves: <prefix>-producer.yaml and <prefix>-consumer.yaml",
        };
        var acknowledgeOption = new Option<bool>("--acknowledge")
        {
            Description = "Confirm the values the cut lists on each side, and write the two files",
        };

        Arguments.Add(jobArgument);
        Options.Add(branchOption);
        Options.Add(rowsOption);
        Options.Add(atOption);
        Options.Add(outOption);
        Options.Add(acknowledgeOption);

        this.SetAction(async (parseResult, ct) => await RunAsync(
            parseResult.GetValue(jobArgument)!,
            parseResult.GetValue(branchOption),
            parseResult.GetValue(rowsOption),
            parseResult.GetValue(atOption),
            parseResult.GetValue(outOption),
            parseResult.GetValue(acknowledgeOption),
            ct));
    }

    private async Task<int> RunAsync(
        string jobPath, string? branch, int rows, int? at, string? outPrefix, bool acknowledge, CancellationToken ct)
    {
        if (!File.Exists(jobPath))
        {
            _console.MarkupLine($"[red]No job file at '{Markup.Escape(jobPath)}'.[/]");
            return 1;
        }
        if (rows < 1)
        {
            _console.MarkupLine("[red]--rows must be at least 1: the cut points are read off a run, and a run of no rows observes no stage.[/]");
            return 1;
        }
        if (at is null != string.IsNullOrEmpty(outPrefix))
        {
            _console.MarkupLine("[red]--at and --out go together: --at names the cut, --out says where the two halves go.[/]");
            return 1;
        }

        var yaml = File.ReadAllText(jobPath);

        // Building the DAG parses YAML, so a malformed job file arrives here as a driver exception.
        // Reporting it as one is what the rest of the CLI does.
        DagBuild build;
        try
        {
            build = DagTopologyService.FromServices(_serviceProvider).Build(yaml);
        }
        catch (Exception ex)
        {
            _console.MarkupLine($"[red]'{Markup.Escape(Path.GetFileName(jobPath))}' is not a job this build can read: "
                              + $"{Markup.Escape(DtPipe.Core.Infrastructure.Diagnostics.ExceptionChainFlattener.Format(ex))}[/]");
            return 1;
        }

        var jobs = build.Jobs;

        var validationErrors = PipelineValidator.Validate(
            build.Dag, jobs,
            _serviceProvider.GetRequiredService<IEnumerable<DtPipe.Core.Abstractions.IStreamTransformerFactory>>()).ToList();
        if (validationErrors.Count > 0)
        {
            foreach (var error in validationErrors)
                _console.MarkupLine($"[red]{Markup.Escape(error)}[/]");
            return 1;
        }

        var target = ResolveBranch(jobs, branch, out var resolveError);
        if (target is null)
        {
            _console.MarkupLine($"[red]{Markup.Escape(resolveError!)}[/]");
            return 1;
        }

        // Everything the gate decides is a property of the job as written, so it is decided before
        // the job is run. A pipeline this refuses to cut is one that would otherwise have opened
        // its source and read rows first — using, in the case the gate exists for, the very
        // credential it is about to complain about.
        if (at is not null && !PassesGate(target, yaml)) return 1;

        // DryRunCount makes it a sample run: the writer is neutralised, and the four hooks, the
        // cursor, the metrics file and the schema migration are suppressed with it.
        jobs[target] = jobs[target] with { DryRunCount = rows };

        var collector = _serviceProvider.GetRequiredService<SampleReportCollector>();
        var jobService = _serviceProvider.GetRequiredService<DtPipe.Cli.JobService>();
        var contexts = jobs.ToDictionary(kv => kv.Key,
                                         kv => new CliJobContext(null, null, null, Array.Empty<string>()),
                                         StringComparer.OrdinalIgnoreCase);

        collector.Clear();
        collector.Enabled = true;
        var failures = new System.Collections.Concurrent.ConcurrentDictionary<string, string>();

        int exitCode;
        try
        {
            exitCode = await jobService.ExecutePipelineAsync(
                jobs, build.Dag, contexts, new GlobalOptions { NoStats = true, Quiet = true }, ct, failures);
        }
        catch (Exception ex)
        {
            _console.MarkupLine($"[red]The job could not be sampled: {Markup.Escape(ex.Message)}[/]");
            return 1;
        }

        if (exitCode != 0)
        {
            foreach (var (alias, reason) in failures)
                _console.MarkupLine($"[red]Branch '{Markup.Escape(alias)}': {Markup.Escape(reason)}[/]");
            _console.MarkupLine("[red]A job that does not run has no cut points to offer.[/]");
            return 1;
        }

        if (!collector.Reports.TryGetValue(target, out var report) || report.Run.Stages.Count == 0)
        {
            _console.MarkupLine($"[red]The sample observed no stage on branch '{Markup.Escape(target)}'.[/]");
            return 1;
        }

        return at is null
            ? Render(target, jobPath, report.Run)
            : Cut(target, jobPath, yaml, report.Run, at.Value, outPrefix!, acknowledge);
    }

    /// <summary>
    /// Re-reads the job <b>as written</b> — every <c>${{…}}</c> left in place.
    /// </summary>
    /// <remarks>
    /// The copy that ran had them resolved, as it must to connect; serialising that one would turn
    /// a reference its author placed to keep a value out of a file into the value itself, in two
    /// files bound for two repositories. It is the same distinction <c>--export-job</c> draws, and
    /// the same one it was found on the wrong side of.
    /// </remarks>
    private Dictionary<string, JobDefinition>? ReadVerbatim(string yaml, out string? error)
    {
        error = null;
        try
        {
            return DtPipe.Configuration.JobFileParser.ParseContent(
                yaml,
                _serviceProvider.GetService<DtPipe.Cli.Security.ISecretsManager>(),
                interpolate: false);
        }
        catch (Exception ex)
        {
            error = $"The job could not be re-read as written: {ex.Message}";
            return null;
        }
    }

    /// <summary>
    /// The two refusals, answered before the job runs. Both are read off the text the author wrote,
    /// so neither needs the sample run that establishes where the stages are.
    /// </summary>
    private bool PassesGate(string alias, string yaml)
    {
        var verbatim = ReadVerbatim(yaml, out var parseError);
        if (verbatim is null)
        {
            _console.MarkupLine($"[red]{Markup.Escape(parseError!)}[/]");
            return false;
        }

        var inspection = JobCutter.Inspect(
            verbatim, alias,
            _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>(),
            _serviceProvider.GetRequiredService<IEnumerable<IDataWriterFactory>>());

        if (inspection.IsCuttable) return true;

        foreach (var block in inspection.UnplaceableOptionBlocks)
            _console.MarkupLine($"[red]provider-options block '{Markup.Escape(block)}' belongs to neither the source nor the target of this branch.[/]");
        if (inspection.UnplaceableOptionBlocks.Count > 0)
            _console.MarkupLine("[red]Which half inherits it is not something the cut can read off the job, and guessing "
                              + "would send it — credentials included — to a repository it does not belong in.[/]");

        if (inspection.Literals.Count > 0)
        {
            foreach (var field in inspection.Literals)
                _console.MarkupLine($"[red]{Markup.Escape(field)} carries a credential in full.[/]");
            _console.WriteLine();
            _console.MarkupLine("[red]Each half of a cut is bound for a different team's repository, so a literal here is "
                              + "copied into two of them. Parameterise it first — [bold]${{ENV_VAR}}[/] or "
                              + "[bold]${{keyring://alias}}[/] — then cut again.[/]");
            _console.MarkupLine("[grey]Blanking it instead would produce a file that looks like a deliverable and cannot run.[/]");
        }

        return false;
    }

    /// <summary>
    /// Writes the two halves, once the run has shown that the cut index names a real stage.
    /// </summary>
    private int Cut(string alias, string jobPath, string yaml, SampleRun run, int at, string outPrefix, bool acknowledge)
    {
        var lastStage = run.Stages.Count - 1;
        if (at < 0 || at > lastStage)
        {
            _console.MarkupLine($"[red]--at {at} is not a stage of branch '{Markup.Escape(alias)}': it has 0 to {lastStage}. "
                              + $"Run 'dtpipe split {Markup.Escape(Path.GetFileName(jobPath))}' to see them.[/]");
            return 1;
        }

        var verbatim = ReadVerbatim(yaml, out var parseError);
        if (verbatim is null)
        {
            _console.MarkupLine($"[red]{Markup.Escape(parseError!)}[/]");
            return 1;
        }

        // The cut moves transformers by position, so a position has to mean the same thing in the
        // file and in the run. It does not when a block produces no transformer — one that sets
        // options without mappings is refused, but one that sets neither is skipped — and a cut
        // placed by number would then move a different stage than the one the candidate named.
        var declared = verbatim[alias].Transformers?.Count ?? 0;
        if (declared != lastStage)
        {
            _console.MarkupLine($"[red]Branch '{Markup.Escape(alias)}' declares {declared} transformers but ran {lastStage}, "
                              + "so a stage number does not name a block of the file. Cutting it would move the wrong stage.[/]");
            return 1;
        }

        var result = JobCutter.Cut(
            verbatim, alias, at,
            _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>(),
            _serviceProvider.GetRequiredService<IEnumerable<IDataWriterFactory>>());

        _console.MarkupLine($"Cutting branch [bold]{Markup.Escape(alias)}[/] of {Markup.Escape(Path.GetFileName(jobPath))} "
                          + $"after {Markup.Escape(at == 0 ? "the reader" : run.Stages[at].Name)}, "
                          + $"{run.Stages[at].Schema.Count} columns crossing on [bold]{JobCutter.Link}[/].");
        _console.WriteLine();

        var producerPath = outPrefix + "-producer.yaml";
        var consumerPath = outPrefix + "-consumer.yaml";

        RenderPlacement(result, producerPath, consumerPath);

        if (!acknowledge)
        {
            _console.WriteLine();
            _console.MarkupLine("Nothing above was recognised as a credential written in full, and that is a scan of the "
                              + "keys this build knows — not a proof that there is none.");
            _console.MarkupLine("A cut does not remove a secret, it redistributes one: where there was one place and one "
                              + "owner, there are now two of each.");
            _console.MarkupLine("Re-run with [bold]--acknowledge[/] to write the two files.");
            return 1;
        }

        File.WriteAllText(producerPath, DtPipe.Configuration.JobFileWriter.Serialize(result.Producer));
        File.WriteAllText(consumerPath, DtPipe.Configuration.JobFileWriter.Serialize(result.Consumer));

        _console.WriteLine();
        _console.MarkupLine($"[green]Written[/] {Markup.Escape(producerPath)} and {Markup.Escape(consumerPath)}.");
        _console.MarkupLine($"[grey]On one host: dtpipe --job {Markup.Escape(producerPath)} | dtpipe --job {Markup.Escape(consumerPath)}[/]");
        return 0;
    }

    /// <summary>Prints what the cut put on each side, and what it carried to neither.</summary>
    private void RenderPlacement(CutResult result, string producerPath, string consumerPath)
    {
        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Half");
        table.AddColumn("Key");
        table.AddColumn("Value");

        string Where(CutSide side) => side switch
        {
            CutSide.Producer => Markup.Escape(producerPath),
            CutSide.Consumer => Markup.Escape(consumerPath),
            _ => "[yellow]neither[/]",
        };

        foreach (var group in result.Placements.GroupBy(p => p.Side))
        {
            foreach (var placement in group)
                table.AddRow(Where(placement.Side), Markup.Escape(placement.Key), Markup.Escape(placement.Display));
        }

        _console.Write(table);

        if (result.Placements.Any(p => p.Side == CutSide.Dropped))
            _console.MarkupLine("[yellow]A cut makes two runs out of one, and one path cannot hold both their reports — "
                              + "give each half its own when you run them.[/]");
    }

    /// <summary>
    /// Prints one row per candidate cut.
    /// </summary>
    /// <remarks>
    /// A cut is named by the stage whose output crosses it, which is the numbering the tap already
    /// uses: 0 is the reader, 1..n the transformers in pipeline order. So cut 0 sends the source
    /// rows untouched and leaves every transformer to the consumer; cut n leaves it the writer
    /// alone.
    ///
    /// <para>
    /// The cost column reads off <see cref="StageCapture.IsColumnar"/>, because the link is Arrow
    /// and always will be: a stage already in Arrow hands its batches to the link as they are,
    /// while a row-mode stage is bridged into Arrow to be written and back out to be read. That is
    /// two bridges the monolithic run did not pay, and it is the only thing separating one
    /// candidate from another that the tool can know.
    /// </para>
    /// </remarks>
    private int Render(string alias, string jobPath, SampleRun run)
    {
        _console.MarkupLine($"Cut points on branch [bold]{Markup.Escape(alias)}[/] of "
                          + $"{Markup.Escape(Path.GetFileName(jobPath))}");
        _console.WriteLine();

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("#");
        table.AddColumn("After");
        table.AddColumn("What crosses");
        table.AddColumn("Link");

        foreach (var stage in run.Stages)
        {
            var columns = stage.Schema.Count == 1 ? "1 column" : $"{stage.Schema.Count} columns";
            var names = string.Join(", ", stage.Schema.Select(c => c.Name));
            table.AddRow(
                stage.Index.ToString(),
                Markup.Escape(stage.Index == 0 ? "the reader" : stage.Name),
                Markup.Escape(names.Length == 0 ? columns : $"{columns} — {names}"),
                stage.IsColumnar ? "[green]free[/]" : "[yellow]two bridges[/]");
        }

        _console.Write(table);
        _console.WriteLine();
        _console.MarkupLine("[grey]Where to cut is not a property of the pipeline: it depends on who owns each side "
                          + "and who pays for the work. This lists what is possible, in the order the stages run.[/]");
        return 0;
    }

    /// <summary>The branch to cut, or null with a message naming what the job actually has.</summary>
    private static string? ResolveBranch(
        Dictionary<string, JobDefinition> jobs, string? requested, out string? error)
    {
        error = null;

        if (!string.IsNullOrEmpty(requested))
        {
            if (jobs.ContainsKey(requested)) return requested;
            error = $"No branch '{requested}' in this job. It has: {string.Join(", ", jobs.Keys.OrderBy(k => k, StringComparer.Ordinal))}.";
            return null;
        }

        var sources = jobs.Where(kv => !string.IsNullOrEmpty(kv.Value.Input))
                          .Select(kv => kv.Key)
                          .OrderBy(k => k, StringComparer.Ordinal)
                          .ToList();

        if (sources.Count == 1) return sources[0];

        error = sources.Count == 0
            ? "No branch of this job reads a source, so there is nothing to cut."
            : $"This job has {sources.Count} branches reading a source ({string.Join(", ", sources)}). "
              + "Name the one to cut with --branch.";
        return null;
    }
}
