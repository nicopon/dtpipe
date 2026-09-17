using System.CommandLine;
using DtPipe.Cli.Pipeline;
using DtPipe.Contracts;
using DtPipe.Core.Models;
using DtPipe.Core.Validation;
using Microsoft.Extensions.DependencyInjection;
using Spectre.Console;

namespace DtPipe.Cli.Commands;

/// <summary>
/// Reads a producer's contract against a consumer, and answers with an exit code.
///
/// <para>
/// <c>check</c> is a <b>sample run</b> whose source is the contract's schema and no rows: the
/// consumer's transformers are built and initialised over the shape the producer promised, the
/// target is inspected, and nothing is written. That is deliberately not <c>--strict-schema</c>,
/// which lives in <c>ValidateAndMigrateAsync</c> — a path sample mode suppresses because it can
/// CREATE and ALTER. Making that flag fire in a preview would change what a preview means for
/// everyone, to serve one command.
/// </para>
/// </summary>
public class ContractCommand : Command
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IAnsiConsole _console;

    public ContractCommand(IServiceProvider serviceProvider, IAnsiConsole console)
        : base("contract", "Check a consumer against a producer's contract")
    {
        _serviceProvider = serviceProvider;
        _console = console;
        Subcommands.Add(CreateCheckCommand());
        Subcommands.Add(CreateDiffCommand());
        Subcommands.Add(CreateShowCommand());
    }

    // ── diff ─────────────────────────────────────────────────────────────────

    private Command CreateDiffCommand()
    {
        var cmd = new Command("diff", "Compare two contracts: does the new one still satisfy the old one's consumers?");

        var oldArgument = new Argument<string>("old") { Description = "The contract consumers were built against" };
        var newArgument = new Argument<string>("new") { Description = "The contract the producer now writes" };
        var jsonOption = new Option<bool>("--json") { Description = "Emit the changes as JSON, for a CI step" };

        cmd.Arguments.Add(oldArgument);
        cmd.Arguments.Add(newArgument);
        cmd.Options.Add(jsonOption);

        cmd.SetAction(parseResult => Diff(
            parseResult.GetValue(oldArgument)!,
            parseResult.GetValue(newArgument)!,
            parseResult.GetValue(jsonOption)));

        return cmd;
    }

    private int Diff(string oldPath, string newPath, bool json)
    {
        foreach (var path in new[] { oldPath, newPath })
            if (!File.Exists(path))
            {
                _console.MarkupLine($"[red]No contract at '{Markup.Escape(path)}'.[/]");
                return 1;
            }

        DataContract older, newer;
        try
        {
            older = DataContract.Read(oldPath);
            newer = DataContract.Read(newPath);
        }
        catch (Exception ex)
        {
            _console.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return 1;
        }

        var changes = ContractDiff.Compare(older, newer);
        var compatible = ContractDiff.IsCompatible(changes);

        if (json)
        {
            var payload = new
            {
                oldHash = older.Hash,
                newHash = newer.Hash,
                compatible,
                changes = changes.Select(c => new
                {
                    column = c.Column,
                    kind = c.Kind.ToString(),
                    breaking = c.Breaking,
                    detail = c.Detail,
                }),
            };
            Console.Out.WriteLine(System.Text.Json.JsonSerializer.Serialize(
                payload, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
            return compatible ? 0 : 1;
        }

        if (older.Hash == newer.Hash)
        {
            _console.MarkupLine($"[green]Identical — {Markup.Escape(older.Hash[..12])}.[/]");
            return 0;
        }

        _console.MarkupLine($"{Markup.Escape(older.Hash[..12])} → {Markup.Escape(newer.Hash[..12])}");
        _console.WriteLine();

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Column");
        table.AddColumn("Change");
        table.AddColumn("What it does to a consumer");

        foreach (var change in changes)
            table.AddRow(
                Markup.Escape(change.Column),
                change.Breaking ? $"[red]{change.Kind}[/]" : $"[green]{change.Kind}[/]",
                Markup.Escape(change.Detail));

        _console.Write(table);
        _console.WriteLine();
        _console.MarkupLine(compatible
            ? "[green]Consumers of the old contract still work.[/]"
            : "[red]This breaks consumers of the old contract.[/]");

        return compatible ? 0 : 1;
    }

    // ── check ────────────────────────────────────────────────────────────────

    private Command CreateCheckCommand()
    {
        var cmd = new Command("check", "Run a consumer job over a contract's schema and report compatibility");

        var jobOption = new Option<string>("--job", "-j") { Description = "Consumer job file (YAML)", Required = true };
        var contractOption = new Option<string>("--contract") { Description = "Contract file the producer wrote", Required = true };
        var branchOption = new Option<string?>("--branch") { Description = "Which branch reads the contract, when the job has more than one source" };

        cmd.Options.Add(jobOption);
        cmd.Options.Add(contractOption);
        cmd.Options.Add(branchOption);

        cmd.SetAction(async (parseResult, ct) =>
        {
            var jobPath = parseResult.GetValue(jobOption)!;
            var contractPath = parseResult.GetValue(contractOption)!;
            var branch = parseResult.GetValue(branchOption);
            return await CheckAsync(jobPath, contractPath, branch, ct);
        });

        return cmd;
    }

    private async Task<int> CheckAsync(string jobPath, string contractPath, string? branch, CancellationToken ct)
    {
        if (!File.Exists(jobPath))
        {
            _console.MarkupLine($"[red]No job file at '{Markup.Escape(jobPath)}'.[/]");
            return 1;
        }
        if (!File.Exists(contractPath))
        {
            _console.MarkupLine($"[red]No contract at '{Markup.Escape(contractPath)}'. "
                              + "The producer writes one with --contract-save.[/]");
            return 1;
        }

        DataContract contract;
        try
        {
            contract = DataContract.Read(contractPath);
        }
        catch (Exception ex)
        {
            _console.MarkupLine($"[red]{Markup.Escape(ex.Message)}[/]");
            return 1;
        }

        // Building the DAG parses YAML, so a malformed job file arrives here as a driver
        // exception. Reporting it as one is what the rest of the CLI does; letting it escape
        // prints YamlDotNet's stack trace at someone whose file has a typo.
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
            build.Dag, jobs, _serviceProvider.GetRequiredService<IEnumerable<DtPipe.Core.Abstractions.IStreamTransformerFactory>>()).ToList();
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

        // The contract replaces the branch's source. DryRunCount makes it a sample run, so the
        // writer is neutralised and the target is inspected rather than migrated.
        jobs[target] = jobs[target] with
        {
            FromContract = contractPath,
            Input = null,
            DryRunCount = 1,
        };

        var collector = _serviceProvider.GetRequiredService<DtPipe.DryRun.SampleReportCollector>();
        var jobService = _serviceProvider.GetRequiredService<DtPipe.Cli.JobService>();
        var contexts = jobs.ToDictionary(kv => kv.Key, kv => new CliJobContext(null, null, null, Array.Empty<string>()),
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
            _console.MarkupLine($"[red]The consumer could not run over this contract: {Markup.Escape(ex.Message)}[/]");
            return 1;
        }

        _console.MarkupLine($"Contract [bold]{Markup.Escape(contract.Hash[..12])}[/] "
                          + $"→ branch [bold]{Markup.Escape(target)}[/] of {Markup.Escape(Path.GetFileName(jobPath))}");
        _console.WriteLine();

        if (exitCode != 0)
        {
            foreach (var (alias, reason) in failures)
                _console.MarkupLine($"[red]Branch '{Markup.Escape(alias)}': {Markup.Escape(reason)}[/]");
            _console.MarkupLine("[red]The consumer does not run over this contract.[/]");
            return 1;
        }

        return RenderVerdict(collector.Reports);
    }

    /// <summary>
    /// Prints every branch's compatibility, column by column, and returns the exit code.
    /// </summary>
    /// <remarks>
    /// A target with no schema to inspect — a CSV file, say — produces no report at all. Saying so
    /// is the honest answer: "compatible" would claim a comparison that never happened, and this
    /// command's whole value is that its exit code means something.
    /// </remarks>
    private int RenderVerdict(IReadOnlyDictionary<string, DtPipe.DryRun.SampleReport> reports)
    {
        var compared = false;
        var compatible = true;

        foreach (var (alias, report) in reports.OrderBy(r => r.Key, StringComparer.Ordinal))
        {
            if (report.CompatibilityReport is not { } compatibility)
                continue;

            compared = true;
            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn($"Column ({Markup.Escape(alias)})");
            table.AddColumn("Contract");
            table.AddColumn("Target");
            table.AddColumn("Verdict");

            foreach (var column in compatibility.Columns)
            {
                var verdict = column.Status switch
                {
                    CompatibilityStatus.Compatible => "[green]ok[/]",
                    CompatibilityStatus.WillBeCreated => "[grey]will be created[/]",
                    _ => $"[red]{column.Status}[/]",
                };
                table.AddRow(
                    Markup.Escape(column.ColumnName),
                    Markup.Escape(column.SourceColumn?.ClrType.Name ?? "—"),
                    Markup.Escape(column.TargetColumn?.NativeType ?? "—"),
                    verdict + (column.Message is null ? "" : $" [grey]{Markup.Escape(column.Message)}[/]"));
            }

            _console.Write(table);

            foreach (var warning in compatibility.Warnings)
                _console.MarkupLine($"[yellow]warning: {Markup.Escape(warning)}[/]");
            foreach (var error in compatibility.Errors)
                _console.MarkupLine($"[red]error: {Markup.Escape(error)}[/]");

            if (!compatibility.IsCompatible) compatible = false;
        }

        if (!compared)
        {
            _console.MarkupLine("[yellow]The consumer initialised over this contract, but its target exposes no "
                              + "schema to compare against — nothing was checked beyond that.[/]");
            return 0;
        }

        _console.WriteLine();
        _console.MarkupLine(compatible
            ? "[green]The consumer accepts this contract.[/]"
            : "[red]The consumer does not accept this contract.[/]");

        // Said on both verdicts, because the green one is where it will be misread.
        _console.MarkupLine("[grey]This compares columns and types. It says nothing about rules the target "
                          + "service enforces in its own code.[/]");

        return compatible ? 0 : 1;
    }

    /// <summary>
    /// Picks the branch the contract feeds: the only one that reads a source, or the named one.
    /// </summary>
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
            ? "No branch of this job reads a source, so there is nothing for a contract to replace."
            : $"This job has {sources.Count} branches reading a source ({string.Join(", ", sources)}). "
              + "Name the one the contract feeds with --branch.";
        return null;
    }

    // ── show ─────────────────────────────────────────────────────────────────

    private Command CreateShowCommand()
    {
        var cmd = new Command("show", "Print what a contract says");
        var pathArgument = new Argument<string>("path") { Description = "Contract file" };
        cmd.Arguments.Add(pathArgument);

        cmd.SetAction(parseResult =>
        {
            var path = parseResult.GetValue(pathArgument)!;
            if (!File.Exists(path))
            {
                _console.MarkupLine($"[red]No contract at '{Markup.Escape(path)}'.[/]");
                return 1;
            }

            var contract = DataContract.Read(path);
            var schema = contract.ToArrowSchema();

            _console.MarkupLine($"[bold]{Markup.Escape(contract.Hash)}[/]");
            _console.MarkupLine($"[grey]source {Markup.Escape(contract.SchemaSource)}"
                              + (contract.Producer is null ? "" : $" · produced by {Markup.Escape(contract.Producer)}")
                              + (contract.Enforcement is null ? "" : $" · source protection {Markup.Escape(contract.Enforcement)}")
                              + "[/]");
            _console.WriteLine();

            var table = new Table().Border(TableBorder.Rounded);
            table.AddColumn("Column");
            table.AddColumn("Type");
            table.AddColumn("Nullable");
            foreach (var field in schema.FieldsList)
                table.AddRow(Markup.Escape(field.Name), Markup.Escape(field.DataType.Name), field.IsNullable ? "yes" : "no");
            _console.Write(table);

            return 0;
        });

        return cmd;
    }
}
