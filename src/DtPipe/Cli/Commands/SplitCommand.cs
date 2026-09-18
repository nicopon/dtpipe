using System.CommandLine;
using DtPipe.Cli.Pipeline;
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

        Arguments.Add(jobArgument);
        Options.Add(branchOption);
        Options.Add(rowsOption);

        this.SetAction(async (parseResult, ct) => await ProposeAsync(
            parseResult.GetValue(jobArgument)!,
            parseResult.GetValue(branchOption),
            parseResult.GetValue(rowsOption),
            ct));
    }

    private async Task<int> ProposeAsync(string jobPath, string? branch, int rows, CancellationToken ct)
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

        // Building the DAG parses YAML, so a malformed job file arrives here as a driver exception.
        // Reporting it as one is what the rest of the CLI does.
        DagBuild build;
        try
        {
            build = DagTopologyService.FromServices(_serviceProvider).Build(File.ReadAllText(jobPath));
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

        return Render(target, jobPath, report.Run);
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
