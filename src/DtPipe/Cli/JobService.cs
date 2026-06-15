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
		rootCommand.Subcommands.Add(new InspectCommand(_serviceProvider));
		rootCommand.Subcommands.Add(new ProvidersCommand(_serviceProvider));
		rootCommand.Subcommands.Add(new CompletionCommand());
		rootCommand.Subcommands.Add(new SecretCommand(_console));
		rootCommand.Subcommands.Add(new McpCommand(_serviceProvider));
		return rootCommand;
	}

	public async Task<int> ExecutePipelineAsync(Dictionary<string, JobDefinition> jobs, JobDagDefinition dag, Dictionary<string, Pipeline.CliJobContext> contexts, Pipeline.GlobalOptions globals, CancellationToken ct)
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
			foreach (var err in validationErrors)
				_console.MarkupLine($"[red]Validation Error:[/] {err}");
			return 1;
		}

		if (dag.IsDag)
		{
			// Dry-run selection logic
			if (globals.DryRunCount > 0 && dag.Branches.Count > 1)
			{
				if (_console.Profile.Capabilities.Interactive && !Console.IsOutputRedirected && !Console.IsInputRedirected)
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

			_console.WriteLine();
			var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
			_console.Write(DagRenderer.BuildTopologyPanel(dag, readerFactories));
			_console.WriteLine();

			var orchestrator = _serviceProvider.GetRequiredService<IDagOrchestrator>();
			orchestrator.OnLogEvent = msg => _console.MarkupLine(msg);

			Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> branchExecutor = async (branch, ctx, token) =>
			{
				var job = jobs[branch.Alias];
				contexts.TryGetValue(branch.Alias, out var branchCtx);
				return await RunSingleJobAsync(job, branchCtx, branch.Alias, true, ctx, resultsCollector, token, globals);
			};

			int exitCode;
			bool isInteractiveLive = !globals.NoStats && globals.DryRunCount == 0 && _console.Profile.Capabilities.Interactive && !Console.IsOutputRedirected && !Console.IsInputRedirected;
			var observer = _serviceProvider.GetRequiredService<IExportObserver>() as DtPipe.Observers.SpectreConsoleObserver;
			
			if (isInteractiveLive && observer != null)
			{
				exitCode = await observer.StartUnifiedLiveDisplayAsync(dag, () => orchestrator.ExecuteAsync(dag, branchExecutor, ct), ct);
			}
			else
			{
				exitCode = await orchestrator.ExecuteAsync(dag, branchExecutor, ct);
			}

			_console.WriteLine();
			DagRenderer.PrintUnifiedResultsTable(resultsCollector.ToList(), dag, isDag: true, _console);
			return exitCode;
		}
		else
		{
			var mainJob = jobs.Values.First();
			_console.WriteLine();
			var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
			_console.Write(DagRenderer.BuildLinearTopologyPanel(mainJob, readerFactories));
			_console.WriteLine();

			var mainContext = contexts.Values.FirstOrDefault();
			return await RunSingleJobAsync(mainJob, mainContext, null, false, null, null, ct, globals);
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
		Pipeline.GlobalOptions? globals = null)
	{
		var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
		registry.BeginScope();

		// Bind options from JobDefinition to the registry (for providers/transformers)
		var providerConfigService = new DtPipe.Cli.Services.ProviderConfigurationService(_contributors, registry);
		providerConfigService.BindOptions(job, context, globals);

		var channelRegistry = _serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
		var linearPipelineService = new DtPipe.Cli.Services.LinearPipelineService(_contributors, _serviceProvider, channelRegistry, registry, _console);
		return await linearPipelineService.ExecuteAsync(job, context, ct, resultsCollector, isDag, alias, ctx, showStatusMessages: false, dryRunInteractiveBranch: globals?.DryRunInteractiveBranch);
	}
}
