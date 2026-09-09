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
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Spectre.Console;

namespace DtPipe.Cli;

public class JobService
{
	private readonly IEnumerable<ICliContributor> _contributors;
	private readonly IServiceProvider _serviceProvider;
	private readonly IAnsiConsole _console;
	private readonly ILoggerFactory _loggerFactory;

	public string[]? RawArgs { get; set; }

	public JobService(
		IServiceProvider serviceProvider,
		IAnsiConsole console,
		ILoggerFactory loggerFactory,
		IEnumerable<IStreamReaderFactory> readerFactories,
		IEnumerable<IDataTransformerFactory> transformerFactories,
		IEnumerable<IDataWriterFactory> writerFactories)
	{
		_serviceProvider = serviceProvider;
		_console = console;
		_loggerFactory = loggerFactory;

		var list = new List<ICliContributor>();
		list.AddRange(readerFactories.OfType<ICliContributor>());
		list.AddRange(transformerFactories.OfType<ICliContributor>());
		list.AddRange(writerFactories.OfType<ICliContributor>());
		_contributors = list;
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
			new McpCommand(_serviceProvider),
			new AgentCommand(_serviceProvider),
		};

	/// <param name="branchFailures">Filled with the reason each failing branch stopped, for a caller
	/// that has silenced the console — an MCP tool reads a result, not a terminal.</param>
	public async Task<int> ExecutePipelineAsync(Dictionary<string, JobDefinition> jobs, JobDagDefinition dag, Dictionary<string, Pipeline.CliJobContext> contexts, Pipeline.GlobalOptions globals, CancellationToken ct,
		System.Collections.Concurrent.ConcurrentDictionary<string, string>? branchFailures = null)
	{
		if (globals.AllFlags.TryGetValue("--cursor-from", out var cursorFromObj) && cursorFromObj is string cursorFromVal && !string.IsNullOrEmpty(cursorFromVal))
		{
			Environment.SetEnvironmentVariable("DTPIPE_CURSOR_OVERRIDE", cursorFromVal);
		}

		// Translate Ctrl-C into cooperative cancellation (F16): the dedicated user token
		// discriminates user shutdown from internal cancellation so the process can report
		// exit code 130 instead of masking cancellation as success.
		using var userCts = new CancellationTokenSource();
		var cancelHandler = new ConsoleCancelEventHandler((_, e) =>
		{
			e.Cancel = true; // graceful shutdown; exit code is reported by the pipeline
			userCts.Cancel();
		});
		Console.CancelKeyPress += cancelHandler;

		try
		{
			var resultsCollector = new System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>();
			
			// Configure logging
			if (!string.IsNullOrEmpty(globals.LogPath))
			{
				Serilog.Log.Logger = new Serilog.LoggerConfiguration()
					.MinimumLevel.Debug()
					.WriteTo.File(globals.LogPath)
					.CreateLogger();
				_loggerFactory.AddSerilog();
			}

			// Validation
			var processorFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
			var validationErrors = Pipeline.PipelineValidator.Validate(dag, jobs, processorFactories);
			if (validationErrors.Any())
			{
				if (!globals.Quiet)
					foreach (var err in validationErrors)
						_console.MarkupLine($"[red]Validation Error:[/] {err}");
				return 1;
			}

			if (dag.IsDag)
			{
				// Dry-run selection logic
				if (globals.DryRunCount > 0 && dag.Branches.Count > 1)
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
						globals.DryRunInteractiveBranch = _console.Prompt(prompt);
					}
					else
					{
						// Fallback to the last branch if not interactive
						globals.DryRunInteractiveBranch = dag.Branches.Last().Alias;
					}
				}

				var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
				if (!globals.Quiet)
				{
					_console.WriteLine();
					_console.Write(DagRenderer.BuildTopologyPanel(dag, readerFactories));
					_console.WriteLine();
				}

				var orchestrator = _serviceProvider.GetRequiredService<IDagOrchestrator>();
				if (!globals.Quiet)
					orchestrator.OnLogEvent = msg => _console.MarkupLine(msg);

				Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> branchExecutor = async (branch, ctx, token) =>
				{
					var job = jobs[branch.Alias];
					contexts.TryGetValue(branch.Alias, out var branchCtx);
					return await RunSingleJobAsync(job, branchCtx, branch.Alias, true, ctx, resultsCollector, token, globals, userCts.Token, branchFailures);
				};

				int exitCode;
				bool isInteractiveLive = !globals.Quiet && !globals.NoStats && globals.DryRunCount == 0 && _console.Profile.Capabilities.Interactive && !Console.IsOutputRedirected && !Console.IsInputRedirected;
				var observer = _serviceProvider.GetRequiredService<IExportObserver>() as DtPipe.Observers.SpectreConsoleObserver;
				
				if (isInteractiveLive && observer != null)
				{
					exitCode = await observer.StartUnifiedLiveDisplayAsync(dag, () => orchestrator.ExecuteAsync(dag, branchExecutor, ct), ct);
				}
				else
				{
					exitCode = await orchestrator.ExecuteAsync(dag, branchExecutor, ct);
				}

				if (!globals.Quiet)
				{
					_console.WriteLine();
					DagRenderer.PrintUnifiedResultsTable(resultsCollector.ToList(), dag, isDag: true, _console);
				}
				return exitCode;
			}
			else
			{
				var mainJob = jobs.Values.First();
				if (!globals.Quiet)
				{
					_console.WriteLine();
					var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
					_console.Write(DagRenderer.BuildLinearTopologyPanel(mainJob, readerFactories));
					_console.WriteLine();
				}

				var mainContext = contexts.Values.FirstOrDefault();

				// P1-8: the linear path goes through DagOrchestrator too — a single-branch
				// DAG gives uniform cancellation, channel wiring and exit-code semantics.
				// Jobs without an output (validation-only mode) keep the direct path.
				if (!string.IsNullOrEmpty(mainJob.Output))
				{
					var linearDag = new JobDagDefinition
					{
						Branches = new[]
						{
							new BranchDefinition
							{
								Alias = "main",
								Input = mainJob.Input,
								Output = mainJob.Output,
								Arguments = Array.Empty<string>()
							}
						}
					};
					var orchestrator = _serviceProvider.GetRequiredService<IDagOrchestrator>();

					Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> linearExecutor =
						(_, branchCtx, token) => RunSingleJobAsync(mainJob, mainContext, "main", isDag: false, branchCtx, null, token, globals, userCts.Token, branchFailures);

					return await orchestrator.ExecuteAsync(linearDag, linearExecutor, ct);
				}

				return await RunSingleJobAsync(mainJob, mainContext, null, false, null, null, ct, globals, userCts.Token, branchFailures);
			}
		}
		finally
		{
			Console.CancelKeyPress -= cancelHandler;
			Environment.SetEnvironmentVariable("DTPIPE_CURSOR_OVERRIDE", null);
		}
	}

	private async Task<int> RunSingleJobAsync(
		JobDefinition job,
		Pipeline.CliJobContext? context,
		string? alias,
		bool isDag,
		BranchChannelContext? ctx,
		System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>? resultsCollector,
		CancellationToken ct,
		Pipeline.GlobalOptions? globals = null,
		CancellationToken userCancellationToken = default,
		System.Collections.Concurrent.ConcurrentDictionary<string, string>? branchFailures = null)
	{
		var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
		registry.BeginScope();

		// BindOptions() below gates flag binding (--query, --table, ...) on factory.CanHandle(job.Input/Output).
		// An unresolved indirection (e.g. "keyring://alias") fails CanHandle for every provider, silently
		// skipping the bind — so resolve a throwaway copy just for that check. LinearPipelineService still
		// does its own resolution later for the actual connection open; that pass is separate, keep it.
		var resolver = _serviceProvider.GetService<DtPipe.Core.Expressions.IStringContentResolver>();
		var jobForBinding = job;
		if (resolver != null)
		{
			var resolvedInput = job.Input != null ? await resolver.ResolveAsync(job.Input, ct) : job.Input;
			var resolvedOutput = job.Output != null ? await resolver.ResolveAsync(job.Output, ct) : job.Output;
			jobForBinding = job with
			{
				Input = resolvedInput ?? job.Input,
				Output = resolvedOutput ?? job.Output
			};
		}

		// Bind options from JobDefinition to the registry (for providers/transformers)
		var providerConfigService = new DtPipe.Cli.Services.ProviderConfigurationService(_contributors, registry);
		providerConfigService.BindOptions(jobForBinding, context, globals);

		var channelRegistry = _serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
		var linearPipelineService = new DtPipe.Cli.Services.LinearPipelineService(_contributors, _serviceProvider, channelRegistry, registry, _console);
		var exitCode = await linearPipelineService.ExecuteAsync(job, context, ct, userCancellationToken, resultsCollector, isDag, alias, ctx, showStatusMessages: false, dryRunInteractiveBranch: globals?.DryRunInteractiveBranch, quiet: globals?.Quiet ?? false);
		if (branchFailures is not null && linearPipelineService.Failure is { } why)
			branchFailures[alias ?? "main"] = why;
		return exitCode;
	}
}
