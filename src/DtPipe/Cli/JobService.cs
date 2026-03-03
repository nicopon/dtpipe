
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
		var inputOption = new Option<string[]>("--input") { Description = "Input connection string, file path, or '-' for stdin" };
		inputOption.Aliases.Add("-i");

		var queryOption = new Option<string[]>("--query") { Description = "SQL query to execute (SELECT only)" };
		queryOption.Aliases.Add("-q");

		var outputOption = new Option<string[]>("--output") { Description = "Output connection string, file path, or '-' for stdout" };
		outputOption.Aliases.Add("-o");

		var connectionTimeoutOption = new Option<int>("--connection-timeout") { Description = "Connection timeout in seconds" };
		connectionTimeoutOption.DefaultValueFactory = _ => 10;

		var queryTimeoutOption = new Option<int>("--query-timeout") { Description = "Query timeout in seconds (0 = no timeout)" };
		queryTimeoutOption.DefaultValueFactory = _ => 0;

		var batchSizeOption = new Option<int>("--batch-size") { Description = "Rows per output batch" };
		batchSizeOption.DefaultValueFactory = _ => 50_000;
		batchSizeOption.Aliases.Add("-b");

		var unsafeQueryOption = new Option<bool>("--unsafe-query") { Description = "Bypass SQL validation" };
		unsafeQueryOption.DefaultValueFactory = _ => false;

		var dryRunOption = new Option<int>("--dry-run") { Description = "Dry-run mode (N rows)", Arity = ArgumentArity.ZeroOrOne };
		dryRunOption.DefaultValueFactory = _ => 0;

		var noStatsOption = new Option<bool>("--no-stats") { Description = "Disable progress bars and stats" };
		noStatsOption.DefaultValueFactory = _ => false;

		var limitOption = new Option<int>("--limit") { Description = "Max rows (0 = unlimited)" };
		limitOption.DefaultValueFactory = _ => 0;

		var samplingRateOption = new Option<double>("--sampling-rate") { Description = "Sampling probability (0.0-1.0)" };
		samplingRateOption.DefaultValueFactory = _ => 1.0;
		samplingRateOption.Aliases.Add("--sample-rate"); // Hidden alias support for backward compatibility

		var samplingSeedOption = new Option<int?>("--sampling-seed") { Description = "Seed for sampling (for reproducibility)" };
		samplingSeedOption.Aliases.Add("--sample-seed");
		var jobOption = new Option<string?>("--job") { Description = "Path to YAML job file" };
		var exportJobOption = new Option<string?>("--export-job") { Description = "Export config to YAML" };
		var logOption = new Option<string?>("--log") { Description = "Path to log file" };
		var keyOption = new Option<string?>("--key") { Description = "Primary Key columns" };

		// Lifecycle Hooks Options
		var preExecOption = new Option<string?>("--pre-exec") { Description = "SQL/Command BEFORE transfer" };
		var postExecOption = new Option<string?>("--post-exec") { Description = "SQL/Command AFTER transfer" };
		var onErrorExecOption = new Option<string?>("--on-error-exec") { Description = "SQL/Command ON ERROR" };
		var finallyExecOption = new Option<string?>("--finally-exec") { Description = "SQL/Command ALWAYS" };

		var strategyOption = new Option<string?>("--strategy") { Description = "Write strategy (Append, Truncate, Recreate, Upsert, Ignore)" };
		strategyOption.Aliases.Add("-s");

		var insertModeOption = new Option<string?>("--insert-mode") { Description = "Insert mode (Standard, Bulk)" };
		var tableOption = new Option<string?>("--table") { Description = "Target table name" };
		tableOption.Aliases.Add("-t");

		var strictSchemaOption = new Option<bool?>("--strict-schema") { Description = "Abort if schema errors found" };
		var noSchemaValidationOption = new Option<bool?>("--no-schema-validation") { Description = "Disable schema check" };

		var metricsPathOption = new Option<string?>("--metrics-path") { Description = "Path to structured metrics JSON output" };
		var autoMigrateOption = new Option<bool?>("--auto-migrate") { Description = "Automatically add missing columns to target table" };

		var maxRetriesOption = new Option<int>("--max-retries") { Description = "Max retries for transient errors" };
		maxRetriesOption.DefaultValueFactory = _ => 3;

		var retryDelayMsOption = new Option<int>("--retry-delay-ms") { Description = "Initial retry delay in ms" };
		retryDelayMsOption.DefaultValueFactory = _ => 1000;

		// DAG Options
		var xstreamerOption = new Option<string[]>("--xstreamer") { Description = "XStreamer provider (e.g. duck)" };
		xstreamerOption.Aliases.Add("-x");

		var aliasOption = new Option<string[]>("--alias") { Description = "Alias(es) for the current DAG branch or streams" };

		// Core Help Options
		var coreOptions = new List<Option> { inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption, unsafeQueryOption, dryRunOption, noStatsOption, limitOption, samplingRateOption, samplingSeedOption, keyOption, jobOption, exportJobOption, logOption, preExecOption, postExecOption, onErrorExecOption, finallyExecOption, strategyOption, insertModeOption, tableOption, maxRetriesOption, retryDelayMsOption, strictSchemaOption, noSchemaValidationOption, metricsPathOption, autoMigrateOption, xstreamerOption, aliasOption };

		var rootCommand = new RootCommand("A simple, self-contained CLI for performance-focused data streaming & anonymization");
		foreach (var opt in coreOptions) rootCommand.Options.Add(opt);

		// Add Contributor Options
		foreach (var contributor in _contributors)
		{
			foreach (var opt in contributor.GetCliOptions())
			{
				if (!rootCommand.Options.Any(o => o.Name == opt.Name || o.Aliases.Contains(opt.Name)))
				{
					rootCommand.Options.Add(opt);
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
				var localInput = pr.GetValue(inputOption)?.FirstOrDefault();
				var localAlias = pr.GetValue(aliasOption)?.FirstOrDefault();

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
					Job = jobOption,
					Input = inputOption,
					Query = queryOption,
					Output = outputOption,
					ConnectionTimeout = connectionTimeoutOption,
					QueryTimeout = queryTimeoutOption,
					BatchSize = batchSizeOption,
					UnsafeQuery = unsafeQueryOption,
					NoStats = noStatsOption,
					Limit = limitOption,
					SamplingRate = samplingRateOption,
					SamplingSeed = samplingSeedOption,
					Log = logOption,
					Key = keyOption,
					PreExec = preExecOption,
					PostExec = postExecOption,
					OnErrorExec = onErrorExecOption,
					FinallyExec = finallyExecOption,
					Strategy = strategyOption,
					InsertMode = insertModeOption,
					Table = tableOption,
					MaxRetries = maxRetriesOption,
					RetryDelayMs = retryDelayMsOption,
					StrictSchema = strictSchemaOption,
					NoSchemaValidation = noSchemaValidationOption,
					MetricsPath = metricsPathOption,
					AutoMigrate = autoMigrateOption,
					Xstreamer = xstreamerOption
				};

				var (job, jobExitCode) = RawJobBuilder.Build(pr, cliJobOptions);

				if (jobExitCode != 0) return jobExitCode;

				job = job with
				{
					Input = ResolveKeyring(job.Input, _console) ?? job.Input,
					Output = ResolveKeyring(job.Output, _console) ?? job.Output
				};

				// Export job
				var exportJobPath = pr.GetValue(exportJobOption);
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
			var logPath = parseResult.GetValue(logOption);
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
