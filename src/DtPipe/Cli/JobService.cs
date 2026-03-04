
using System.CommandLine;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Commands;
using DtPipe.Cli.Dag;
using DtPipe.Cli.Security;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Validation;
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

	public JobService(
		IServiceProvider serviceProvider,
		IAnsiConsole console,
		ILoggerFactory loggerFactory,
		IEnumerable<IStreamReaderFactory> readerFactories,
		IEnumerable<IDataTransformerFactory> transformerFactories,
		IEnumerable<IDataWriterFactory> writerFactories,
		IEnumerable<IXStreamerFactory> xstreamerFactories)
	{
		_serviceProvider = serviceProvider;
		_console = console;
		_loggerFactory = loggerFactory;

		// Aggregate all contributors
		var list = new List<ICliContributor>();
		list.AddRange(readerFactories.OfType<ICliContributor>());
		list.AddRange(transformerFactories.OfType<ICliContributor>());
		list.AddRange(writerFactories.OfType<ICliContributor>());
		list.AddRange(xstreamerFactories.OfType<ICliContributor>());
		_contributors = list;
	}

	public (RootCommand, Action) Build()
	{
		var opts = CoreOptionsBuilder.Build();
		var coreOptions = opts.AllOptions;

		var rootCommand = new RootCommand("A simple, self-contained CLI for performance-focused data streaming & anonymization");
		foreach (var opt in coreOptions) rootCommand.Add(opt);

		// Add Contributor Options
		foreach (var contributor in _contributors)
		{
			foreach (var opt in contributor.GetCliOptions())
			{
				if (!rootCommand.Options.Any(o => o.Name == opt.Name || o.Aliases.Contains(opt.Name)))
				{
					rootCommand.Add(opt);
				}
			}
		}

        if (Environment.GetEnvironmentVariable("DEBUG") == "1")
        {
            Console.WriteLine("--- Registered CLI Options ---");
            foreach (var opt in rootCommand.Options)
            {
                Console.WriteLine($"Option: {opt.Name} (Aliases: {string.Join(", ", opt.Aliases)})");
            }
            Console.WriteLine("------------------------------");
        }

		// Add Inspect Command
		rootCommand.Subcommands.Add(new InspectCommand(_serviceProvider));

		rootCommand.Subcommands.Add(new ProvidersCommand(_serviceProvider));

		// Add Secret Command
		rootCommand.Subcommands.Add(new SecretCommand());
		rootCommand.Subcommands.Add(new EngineDuckDbCommand());

		rootCommand.SetAction(async (ParseResult parseResult, CancellationToken ct) =>
		{
			// 1. Check if we have a DAG
			var rawArgs = Environment.GetCommandLineArgs().Skip(1).ToArray(); // Skip executable path

			if (parseResult.Errors.Count > 0)
			{
				foreach (var error in parseResult.Errors)
				{
					Console.Error.WriteLine($"CLI Error: {error.Message}");
				}
			}

			var dagDefinition = CliDagParser.Parse(rawArgs);

			Func<ParseResult, CancellationToken, string[], Task<int>> executePipeline = async (pr, token, currentRawArgs) =>
			{
				// 1. Recover Scalar Value for branch context
				var localInput = pr.GetValue(opts.Input)?.FirstOrDefault();
				var localAlias = pr.GetValue(opts.Alias)?.FirstOrDefault();

				// Perform preliminary contributor actions
				foreach (var contributor in _contributors)
				{
					var exitCode = await contributor.HandleCommandAsync(pr, token);
					if (exitCode.HasValue) return exitCode.Value;
				}

				// Check for deprecated options/prefixes
				CheckDeprecations(pr, _console, currentRawArgs);

				// Build Job Definition
				var cliJobOptions = new DtPipe.Cli.Infrastructure.CliJobOptions
				{
					Job = opts.Job,
					Input = opts.Input,
					Query = opts.Query,
					Output = opts.Output,
					ConnectionTimeout = opts.ConnectionTimeout,
					QueryTimeout = opts.QueryTimeout,
					BatchSize = opts.BatchSize,
					UnsafeQuery = opts.UnsafeQuery,
					NoStats = opts.NoStats,
					Limit = opts.Limit,
					SamplingRate = opts.SamplingRate,
					SamplingSeed = opts.SamplingSeed,
					Log = opts.Log,
					Key = opts.Key,
					PreExec = opts.PreExec,
					PostExec = opts.PostExec,
					OnErrorExec = opts.OnErrorExec,
					FinallyExec = opts.FinallyExec,
					Strategy = opts.Strategy,
					InsertMode = opts.InsertMode,
					Table = opts.Table,
					MaxRetries = opts.MaxRetries,
					RetryDelayMs = opts.RetryDelayMs,
					StrictSchema = opts.StrictSchema,
					NoSchemaValidation = opts.NoSchemaValidation,
					MetricsPath = opts.MetricsPath,
					AutoMigrate = opts.AutoMigrate,
					Xstreamer = opts.Xstreamer
				};

				var (job, jobExitCode) = RawJobBuilder.Build(pr, cliJobOptions);

				if (jobExitCode != 0) return jobExitCode;

				job = job with
				{
					Input = ResolveKeyring(job.Input, _console) ?? job.Input,
					Output = ResolveKeyring(job.Output, _console) ?? job.Output
				};

				// Export job
				var exportJobPath = pr.GetValue(opts.ExportJob);
				if (!string.IsNullOrWhiteSpace(exportJobPath))
				{
					var factoryList = _contributors.OfType<IDataTransformerFactory>().ToList();
					var configs = RawJobBuilder.BuildTransformerConfigsFromCli(
						currentRawArgs,
						factoryList,
						_contributors);

					job = job with { Transformers = configs };
					JobFileWriter.Write(exportJobPath, job);
					return 0;
				}

				var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();

				var providerConfigService = new DtPipe.Cli.Services.ProviderConfigurationService(_contributors, registry);
				providerConfigService.BindOptions(job, pr);

				var linearPipelineService = new DtPipe.Cli.Services.LinearPipelineService(_contributors, _serviceProvider, registry, _console);
				return await linearPipelineService.ExecuteAsync(job, currentRawArgs, token, localAlias, dagDefinition.IsDag);
			};
			// Initialize logging early for DAG execution
			var logPath = parseResult.GetValue(opts.Log);
			if (!string.IsNullOrEmpty(logPath))
			{
				Serilog.Log.Logger = new Serilog.LoggerConfiguration()
					.MinimumLevel.Debug()
					.WriteTo.File(logPath)
					.CreateLogger();
				_loggerFactory.AddSerilog();
			}

			if (dagDefinition.IsDag)
			{
				var topologyErrors = CliDagParser.Validate(dagDefinition);
				if (topologyErrors.Count > 0)
				{
					foreach (var err in topologyErrors)
						_console.MarkupLine($"[red]DAG topology error:[/] {err}");
					return 1;
				}

				_console.WriteLine();
				var tree = new Tree("[yellow]🔀 Pipeline DAG Topology[/]");
				foreach (var branch in dagDefinition.Branches)
				{
					if (branch.IsXStreamer)
						tree.AddNode($"[magenta]⚙ XStreamer:[/] {branch.Alias}");
					else
						tree.AddNode($"[blue]📄 Source:[/] {branch.Alias}");
				}
				_console.Write(tree);
				_console.WriteLine();

				try
				{
					var orchestrator = _serviceProvider.GetRequiredService<IDagOrchestrator>();
					orchestrator.OnLogEvent = msg => _console.MarkupLine(msg);

					Func<string[], CancellationToken, Task<int>> branchExecutor = async (branchArgs, token) =>
					{
						var branchPr = rootCommand.Parse(branchArgs);
						if (branchPr.Errors.Count > 0)
						{
							_console.Write(new Spectre.Console.Markup($"[red]Error parsing branch arguments:[/]{Environment.NewLine}"));
							foreach(var err in branchPr.Errors) _console.WriteLine(err.Message);
							return 1;
						}
						// Suppress log file setup on branches, they will inherit global settings or orchestrator will capture
						return await executePipeline(branchPr, token, branchArgs);
					};

					return await orchestrator.ExecuteAsync(dagDefinition, branchExecutor, ct);
				}
				catch (Exception ex)
				{
					_console.Write(new Spectre.Console.Markup($"{Environment.NewLine}[red]Error:[/] {Spectre.Console.Markup.Escape(ex.Message)}{Environment.NewLine}"));
					if (Environment.GetEnvironmentVariable("DEBUG") == "1")
					{
						_console.WriteException(ex);
					}
					return 1;
				}
			}

			// Execution for non-DAG
			return await executePipeline(parseResult, ct, rawArgs);
		});

		Action printHelp = () => PrintGroupedHelp(rootCommand, coreOptions, _contributors, _console);
		return (rootCommand, printHelp);
	}

	private static void PrintGroupedHelp(RootCommand rootCommand, List<Option> coreOptions, IEnumerable<ICliContributor> contributors, IAnsiConsole console)
	{
		console.WriteLine("Description:");
		console.WriteLine($"  {rootCommand.Description}");
		console.WriteLine();
		console.WriteLine("Usage:");
		console.WriteLine("  dtpipe [options]");
		console.WriteLine();

		console.WriteLine("Core Options:");
		foreach (var opt in coreOptions) PrintOption(opt, console);
		console.WriteLine();

		var groups = contributors.GroupBy(c => c.Category).OrderBy(g => g.Key);

		foreach (var group in groups)
		{
			console.WriteLine($"{group.Key}:");
			// Collect all options for this group
			var optionsPrinted = new HashSet<string>();
			foreach (var contributor in group)
			{
				foreach (var opt in contributor.GetCliOptions())
				{
					if (optionsPrinted.Add(opt.Name))
					{
						PrintOption(opt, console);
					}
				}
			}
			console.WriteLine();
		}

		console.WriteLine("Other Options:");
		console.WriteLine("  -?, -h, --help                           Show this help");
		console.WriteLine("  --version                                Show version");
		console.WriteLine();
	}

	private static void PrintOption(Option opt, IAnsiConsole console)
	{
		if (opt.Description?.StartsWith("[HIDDEN]") == true) return;

		var allAliases = new HashSet<string> { opt.Name };
		foreach (var alias in opt.Aliases)
		{
			// Hide deprecated sampling aliases from help
			if (alias.Equals("--sample-rate", StringComparison.OrdinalIgnoreCase) ||
			    alias.Equals("--sample-seed", StringComparison.OrdinalIgnoreCase))
			{
				continue;
			}
			allAliases.Add(alias);
		}

		var name = string.Join(", ", allAliases.OrderByDescending(a => a.Length));
		var desc = opt.Description ?? "";
		console.WriteLine($"  {name,-40} {desc}");
	}

	// Method moved to LinearPipelineService

	private static string? ResolveKeyring(string? input, IAnsiConsole console)
	{
		if (string.IsNullOrWhiteSpace(input)) return input;

		const string prefix = "keyring://";
		if (input.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
		{
			var alias = input.Substring(prefix.Length).Trim();
			try
			{
				var mgr = new SecretsManager();
				var secret = mgr.GetSecret(alias);
				if (secret == null)
				{
					console.MarkupLine($"[red]Error: Secret alias '{alias}' not found in keyring.[/]");
					Environment.Exit(1);
				}
				console.MarkupLine($"[grey]Resolved keyring secret for alias: {alias}[/]");
				return secret;
			}
			catch (Exception ex)
			{
				console.MarkupLine($"[red]Error resolving keyring secret: {ex.Message}[/]");
				return null;
			}
		}

		return input;
	}

	private static void CheckDeprecations(ParseResult parseResult, IAnsiConsole console, string[] args)
	{

		// 1. Check for --sample-rate or --sample-seed
		if (args.Any(a => a.Equals("--sample-rate", StringComparison.OrdinalIgnoreCase)))
		{
			console.Write(new Markup($"[yellow]Warning: Option '--sample-rate' is deprecated and will be removed in a future version. Please use '--sampling-rate' instead.[/]{Environment.NewLine}"));
		}
		if (args.Any(a => a.Equals("--sample-seed", StringComparison.OrdinalIgnoreCase)))
		{
			console.Write(new Markup($"[yellow]Warning: Option '--sample-seed' is deprecated and will be removed in a future version. Please use '--sampling-seed' instead.[/]{Environment.NewLine}"));
		}

		// 2. Check for sample: prefix in -i/--input
		// Identify any argument starting with sample:
		// However, to be more precise, we should check if it follows -i or --input
		for (int i = 0; i < args.Length; i++)
		{
			var arg = args[i];
			if (arg.Equals("-i", StringComparison.OrdinalIgnoreCase) || arg.Equals("--input", StringComparison.OrdinalIgnoreCase))
			{
				if (i + 1 < args.Length && args[i + 1].StartsWith("sample:", StringComparison.OrdinalIgnoreCase))
				{
					console.Write(new Markup($"[yellow]Warning: Provider prefix 'sample:' is deprecated and will be removed in a future version. Please use 'generate:' instead.[/]{Environment.NewLine}"));
					break;
				}
			}
		}
	}
}
