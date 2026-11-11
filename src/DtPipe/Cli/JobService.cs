using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Commands;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Engine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Spectre.Console;

namespace DtPipe.Cli;

/// <summary>
/// The command line's host of a run: its subcommands, and what a run shows and how it is interrupted
/// (<see cref="IRunHost"/>). <see cref="JobExecutor"/> runs the job.
/// </summary>
public class JobService : IRunHost
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IAnsiConsole _console;
    private readonly ILoggerFactory _loggerFactory;
    private readonly JobExecutor _executor;

    public string[]? RawArgs { get; set; }

    public JobService(
        IServiceProvider serviceProvider,
        IAnsiConsole console,
        ILoggerFactory loggerFactory,
        JobExecutor executor)
    {
        _serviceProvider = serviceProvider;
        _console = console;
        _loggerFactory = loggerFactory;
        _executor = executor;
    }

    public RootCommand BuildSubcommands()
    {
        var rootCommand = new RootCommand("A simple, self-contained CLI for performance-focused data streaming & anonymization");
        foreach (var sub in CreateSubcommands())
            rootCommand.Subcommands.Add(sub);
        return rootCommand;
    }

    public IReadOnlyList<Command> Subcommands => CreateSubcommands();

    private IReadOnlyList<Command> CreateSubcommands()
        => new Command[]
        {
            new InspectCommand(_serviceProvider),
            new ProvidersCommand(_serviceProvider),
            new CompletionCommand(),
            new SecretCommand(_console, _serviceProvider.GetRequiredService<DtPipe.Cli.Security.ISecretsManager>()),
            new SessionCommand(_console),
            new ContractCommand(_serviceProvider, _console),
            new SplitCommand(_serviceProvider, _console),
            new McpCommand(_serviceProvider),
            new AgentCommand(_serviceProvider),
            new StudioCommand(_serviceProvider),
            new NodeCommand(_serviceProvider),
            new KeygenCommand(),
        };

    /// <param name="branchFailures">Filled with the reason each failing branch stopped, for a caller
    /// that has silenced the console — an MCP tool reads a result, not a terminal.</param>
    public Task<int> ExecutePipelineAsync(Dictionary<string, JobDefinition> jobs, JobDagDefinition dag, Dictionary<string, Pipeline.CliJobContext> contexts, Pipeline.GlobalOptions globals, CancellationToken ct,
        System.Collections.Concurrent.ConcurrentDictionary<string, string>? branchFailures = null)
        => _executor.ExecuteAsync(jobs, dag, contexts, globals, this, ct, branchFailures);

    IDisposable IRunHost.BeginRun(Pipeline.GlobalOptions globals, CancellationTokenSource userCts)
    {
        // Translate Ctrl-C into cooperative cancellation (F16).
        var cancelHandler = new ConsoleCancelEventHandler((_, e) =>
        {
            e.Cancel = true; // graceful shutdown; exit code is reported by the pipeline
            userCts.Cancel();
        });
        Console.CancelKeyPress += cancelHandler;
        var stopListening = new CancelKeyRegistration(cancelHandler);

        try
        {
            // Configure logging
            if (!string.IsNullOrEmpty(globals.LogPath))
            {
                Serilog.Log.Logger = new Serilog.LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .WriteTo.File(globals.LogPath)
                    .CreateLogger();
                _loggerFactory.AddSerilog();
            }
        }
        catch
        {
            stopListening.Dispose();
            throw;
        }

        return stopListening;
    }

    private sealed class CancelKeyRegistration(ConsoleCancelEventHandler handler) : IDisposable
    {
        public void Dispose() => Console.CancelKeyPress -= handler;
    }

    string IRunHost.ChooseSampleBranch(JobDagDefinition dag)
    {
        // An LLM-driven dry-run (dtpipe agent's in-process MCP call) shares this
        // process's real console with the agent's own TUI, so the capability check
        // alone reads as interactive here too — NonInteractiveGuard is the only thing
        // that tells the two apart (see its doc comment).
        if (!NonInteractiveGuard.IsSuppressed && _console.Profile.Capabilities.Interactive && !Console.IsOutputRedirected && !Console.IsInputRedirected)
        {
            var prompt = new SelectionPrompt<string>()
                .Title("Select branch to inspect for dry-run:")
                .AddChoices(dag.Branches.Select(b => b.Alias));
            return _console.Prompt(prompt);
        }
        else
        {
            // Fallback to the last branch if not interactive
            return dag.Branches.Last().Alias;
        }
    }

    void IRunHost.ShowTopology(JobDagDefinition dag)
    {
        var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
        _console.WriteLine();
        _console.Write(DagRenderer.BuildTopologyPanel(dag, readerFactories));
        _console.WriteLine();
    }

    void IRunHost.ShowTopology(JobDefinition job)
    {
        _console.WriteLine();
        var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
        _console.Write(DagRenderer.BuildLinearTopologyPanel(job, readerFactories));
        _console.WriteLine();
    }

    Task<int> IRunHost.RunDagAsync(JobDagDefinition dag, Pipeline.GlobalOptions globals, Func<Task<int>> run, CancellationToken ct)
    {
        bool isInteractiveLive = !globals.Quiet && !globals.NoStats && globals.DryRunCount == 0 && _console.Profile.Capabilities.Interactive && !Console.IsOutputRedirected && !Console.IsInputRedirected;
        var observer = _serviceProvider.GetRequiredService<IExportObserver>() as DtPipe.Observers.SpectreConsoleObserver;

        if (isInteractiveLive && observer != null)
            return observer.StartUnifiedLiveDisplayAsync(dag, run, ct);
        return run();
    }

    void IRunHost.ShowResults(IReadOnlyList<DtPipe.Feedback.BranchSummary> results, JobDagDefinition dag, int exitCode)
    {
        _console.WriteLine();
        DagRenderer.PrintUnifiedResultsTable(results.ToList(), dag, isDag: true, _console, exitCode);
    }

    void IRunHost.ShowRefusal(RunRefusal refusal, IEnumerable<string> errors)
    {
        var label = refusal == RunRefusal.Binding ? "Binding Error:" : "Validation Error:";
        foreach (var err in errors)
            _console.MarkupLine($"[red]{label}[/] {Markup.Escape(err)}");
    }

    void IRunHost.ShowDagEvent(DtPipe.Core.Abstractions.Dag.DagLogEvent dagEvent)
    {
        var glyph = dagEvent.Severity == DtPipe.Core.Abstractions.Dag.DagLogSeverity.Error ? "✖" : "⚠";
        var indent = dagEvent.Branch is null ? "" : "  ";
        _console.MarkupLine(Markup.Escape($"{indent}{glyph} {dagEvent.Message}"));
    }

    void IRunHost.ShowBranchFault(string reason, Exception fault)
    {
        _console.Write(new Markup($"{Environment.NewLine}[red]Error: {Markup.Escape(reason)}[/]{Environment.NewLine}"));
        if (Environment.GetEnvironmentVariable("DEBUG") == "1")
            _console.WriteLine(fault.StackTrace ?? "");
    }

    void IRunHost.ShowUserCancellation()
        => _console.Write(new Markup($"{Environment.NewLine}[yellow]Warning: Pipeline canceled by user (Ctrl-C).[/]{Environment.NewLine}"));
}
