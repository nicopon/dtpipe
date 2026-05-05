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
		return rootCommand;
	}

	public async Task<int> ExecutePipelineAsync(Dictionary<string, JobDefinition> jobs, JobDagDefinition dag, Pipeline.GlobalOptions globals, CancellationToken ct)
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
			// Dry-run selection logic if needed (skipped for now as per minimal Strategy D)
			// But let's implement the core execution
			_console.WriteLine();
			var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
			_console.Write(DagRenderer.BuildTopologyPanel(dag, readerFactories));
			_console.WriteLine();

			var orchestrator = _serviceProvider.GetRequiredService<IDagOrchestrator>();
			orchestrator.OnLogEvent = msg => _console.MarkupLine(msg);

			Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> branchExecutor = async (branch, ctx, token) =>
			{
				var job = jobs[branch.Alias];
				return await RunSingleJobAsync(job, branch.Arguments, branch.Alias, true, ctx, resultsCollector, token);
			};

			var exitCode = await orchestrator.ExecuteAsync(dag, branchExecutor, ct);
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

			return await RunSingleJobAsync(mainJob, Array.Empty<string>(), null, false, null, resultsCollector, ct);
		}
	}

	private async Task<int> RunSingleJobAsync(
		JobDefinition job, 
		string[] args, 
		string? alias, 
		bool isDag, 
		BranchChannelContext? ctx, 
		System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary> resultsCollector,
		CancellationToken ct)
	{
		var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
		registry.BeginScope();

		// Bind options from JobDefinition to the registry (for providers/transformers)
		var providerConfigService = new DtPipe.Cli.Services.ProviderConfigurationService(_contributors, registry);
		providerConfigService.BindOptions(job);

		var channelRegistry = _serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
		var linearPipelineService = new DtPipe.Cli.Services.LinearPipelineService(_contributors, _serviceProvider, channelRegistry, registry, _console);
		return await linearPipelineService.ExecuteAsync(job, args, ct, resultsCollector, isDag, alias, ctx, showStatusMessages: false);
	}

    // Legacy method for back-compat or help, to be cleaned up later
	public (RootCommand Command, Action PrintHelp, CoreCliOptions CoreOptions, IReadOnlyDictionary<string, CliPipelinePhase> FlagPhases, IReadOnlyList<ICliContributor> Contributors) Build()
	{
        // This is the old Build() used by Program.cs and Help
        // For now I'll keep a minimal version or just use it for Help.
        var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
        var writerFactories = _serviceProvider.GetRequiredService<IEnumerable<IDataWriterFactory>>();
        var opts = CoreOptionsBuilder.Build(readerFactories, writerFactories);
        var rootCommand = BuildSubcommands();
        
        // Add options to root command just for help generation
        foreach (var opt in opts.AllOptions) rootCommand.Add(opt);
        foreach (var c in _contributors) foreach(var o in c.GetCliOptions()) if (!rootCommand.Options.Any(x => x.Name == o.Name)) rootCommand.Add(o);

        Action PrintHelpAction = () => {
            rootCommand.Parse(new[] { "--help" }).Invoke();
        };
        
        return (rootCommand, PrintHelpAction, opts, CoreOptionsBuilder.CoreFlagPhases, _contributors.ToList());
	}
}
