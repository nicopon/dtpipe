
using System.CommandLine;
using System.CommandLine.Parsing;
using System.CommandLine.Completions;
using DtPipe.Cli.Helpers;
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
using DtPipe.Core.Pipelines.Dag;
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
		IEnumerable<IDataWriterFactory> writerFactories)
	{
		_serviceProvider = serviceProvider;
		_console = console;
		_loggerFactory = loggerFactory;

		// Aggregate all contributors
		var list = new List<ICliContributor>();
		list.AddRange(readerFactories.OfType<ICliContributor>());
		list.AddRange(transformerFactories.OfType<ICliContributor>());
		list.AddRange(writerFactories.OfType<ICliContributor>());
		_contributors = list;
	}

	public (RootCommand Command, Action PrintHelp, CoreCliOptions CoreOptions, IReadOnlyDictionary<string, CliPipelinePhase> FlagPhases, IReadOnlyList<ICliContributor> Contributors) Build()
	{
		var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
		var writerFactories = _serviceProvider.GetRequiredService<IEnumerable<IDataWriterFactory>>();
		var opts = CoreOptionsBuilder.Build(readerFactories, writerFactories);
		var coreOptions = opts.AllOptions;
		var coreCoreOptions = coreOptions.ToList(); // Snapshot before contributors are added

		var rootCommand = new RootCommand("A simple, self-contained CLI for performance-focused data streaming & anonymization");
		foreach (var opt in coreOptions) rootCommand.Add(opt);


		var allFlagPhases = new Dictionary<string, CliPipelinePhase>(StringComparer.OrdinalIgnoreCase);

		// 1. Core flag phases
		foreach (var kv in CoreOptionsBuilder.CoreFlagPhases)
			allFlagPhases.TryAdd(kv.Key, kv.Value);

		// Add Contributor Options
		foreach (var contributor in _contributors)
		{
			// Add specific contributor phase mappings
			if (contributor.FlagPhases != null)
			{
				foreach (var kv in contributor.FlagPhases)
					allFlagPhases.TryAdd(kv.Key, kv.Value);
			}

			foreach (var opt in contributor.GetCliOptions())
			{
				if (opt.Description?.StartsWith("[HIDDEN]", StringComparison.OrdinalIgnoreCase) == true) opt.Hidden = true;

				if (!rootCommand.Options.Any(o => o.Name == opt.Name || o.Aliases.Contains(opt.Name)))
				{
					rootCommand.Add(opt);
					opts.AllOptions.Add(opt); // Ensure AllOptions includes contributor-provided flags
				}
			}
		}

		// Add Inspect Command
		rootCommand.Subcommands.Add(new InspectCommand(_serviceProvider));

		rootCommand.Subcommands.Add(new ProvidersCommand(_serviceProvider));
		rootCommand.Subcommands.Add(new CompletionCommand());
		rootCommand.Subcommands.Add(new SecretCommand(_console));

		rootCommand.SetAction(async (ParseResult parseResult, CancellationToken ct) =>
		{
			// 1. Check if we have a DAG
			var rawArgs = Environment.GetCommandLineArgs().Skip(1).ToArray(); // Skip executable path

			if (rawArgs.Length == 0) return; // Should already be handled in Program.cs, but just in case.

             // Note: We don't check for global parseResult.Errors here because 
             // for multi-branch DAGs, the full command line contains repeated options
             // that System.CommandLine considers errors at the root level.
             // Branch-level validation happens inside the executor.

				// 2. Build Initial Job(s) and detect DAG vs Linear
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
				StrictSchema = opts.StrictSchema,
				NoSchemaValidation = opts.NoSchemaValidation,
				MetricsPath = opts.MetricsPath,
				AutoMigrate = opts.AutoMigrate,
				Sql = opts.Sql,
				Prefix = opts.Prefix,
				ExportJob = opts.ExportJob,
				Rename = opts.Rename,
				Drop = opts.Drop,
				Throttle = opts.Throttle,
				IgnoreNulls = opts.IgnoreNulls,
				From = opts.From,
				Merge = opts.Merge,
				Ref = opts.Ref,
				SchemaSave = opts.SchemaSave,
				SchemaLoad = opts.SchemaLoad,
				Path = opts.Path,
				ColumnTypes = opts.ColumnTypes,
				AutoColumnTypes = opts.AutoColumnTypes,
				MaxSample = opts.MaxSample,
				Encoding = opts.Encoding
			};

			Dictionary<string, JobDefinition> jobs;
			bool isDagYaml = false;
			JobDagDefinition dagDefinition;

			var jobFilePath = parseResult.GetValue(opts.Job);
			if (!string.IsNullOrEmpty(jobFilePath))
			{
				var (j, jec) = RawJobBuilder.Build(parseResult, cliJobOptions);
				if (jec != 0) { Environment.ExitCode = jec; return; }
				jobs = j;
				isDagYaml = jobs.Count > 1 || jobs.Values.Any(j => !string.IsNullOrEmpty(j.Sql) || !string.IsNullOrEmpty(j.From));
				
				var branches = new List<BranchDefinition>();
				foreach (var kvp in jobs)
				{
						var args = new List<string> { "--alias", kvp.Key };
						if (!string.IsNullOrEmpty(kvp.Value.Input)) { args.Add("--input"); args.Add(kvp.Value.Input); }
						if (!string.IsNullOrEmpty(kvp.Value.Output)) { args.Add("--output"); args.Add(kvp.Value.Output); }
						if (!string.IsNullOrEmpty(kvp.Value.Query)) { args.Add("--query"); args.Add(kvp.Value.Query); }
						if (!string.IsNullOrEmpty(kvp.Value.Table)) { args.Add("--table"); args.Add(kvp.Value.Table); }
						if (!string.IsNullOrEmpty(kvp.Value.Sql))
						{
							args.Add("--sql");
							args.Add(kvp.Value.Sql ?? "");
						}
						foreach (var r in kvp.Value.Ref ?? Array.Empty<string>()) { args.Add("--ref"); args.Add(r); }
						if (!string.IsNullOrEmpty(kvp.Value.From)) { args.Add("--from"); args.Add(kvp.Value.From); }

						branches.Add(new BranchDefinition
						{
							Alias = kvp.Key,
							Input = kvp.Value.Input,
							Output = kvp.Value.Output,
							ProcessorName = !string.IsNullOrEmpty(kvp.Value.Sql) ? "sql" : null,
							StreamingAliases = !string.IsNullOrEmpty(kvp.Value.From) ? new[] { kvp.Value.From } : Array.Empty<string>(),
							RefAliases = (kvp.Value.Ref ?? Array.Empty<string>()).ToList(),
							PreParsedJob = kvp.Value,
							Arguments = args.ToArray()
						});
				}
				dagDefinition = new JobDagDefinition { Branches = branches };
			}
			else
			{
				dagDefinition = CliDagParser.Parse(rawArgs);
				if (dagDefinition.IsDag)
				{
					if (Environment.GetEnvironmentVariable("DEBUG") == "1")
						Console.Error.WriteLine($"[DEBUG] Detected DAG with {dagDefinition.Branches.Count} branches");

					jobs = new Dictionary<string, JobDefinition>();
					var factoryList = _contributors.OfType<IDataTransformerFactory>().ToList();

					foreach (var branch in dagDefinition.Branches)
					{
						if (Environment.GetEnvironmentVariable("DEBUG") == "1")
							Console.Error.WriteLine($"[DEBUG] Parsing branch '{branch.Alias}' with args: {string.Join(" ", branch.Arguments)}");

						var branchPr = rootCommand.Parse(branch.Arguments);
						var (branchJobs, branchJec) = RawJobBuilder.Build(branchPr, cliJobOptions);
						if (branchJec != 0) { Environment.ExitCode = branchJec; return; }
						var bj = branchJobs.Values.First();
						
						bj = bj with {
							Ref = branch.RefAliases?.ToArray() ?? Array.Empty<string>(),
							From = branch.StreamingAliases.FirstOrDefault(),
							Sql = DtPipe.Cli.Dag.CliDagParser.ExtractArgValue(branch.Arguments, "--sql"),
							Transformers = RawJobBuilder.BuildTransformerConfigsFromCli(branch.Arguments, factoryList, _contributors)
						};
						jobs[branch.Alias] = bj;
						branch.PreParsedJob = bj;
					}
				}
				else
				{
					var (j, jec) = RawJobBuilder.Build(parseResult, cliJobOptions);
					if (jec != 0) { Environment.ExitCode = jec; return; }
					jobs = j;
					
					// Populate transformers for linear case if we might export
					var factoryList = _contributors.OfType<IDataTransformerFactory>().ToList();
					var mainJob = jobs.Values.First();
					jobs["main"] = mainJob with { 
						Transformers = RawJobBuilder.BuildTransformerConfigsFromCli(rawArgs, factoryList, _contributors)
					};
				}
			}

			// 3. Export job if requested
			var exportJobPath = parseResult.GetValue(opts.ExportJob);
			if (!string.IsNullOrWhiteSpace(exportJobPath))
			{
				// Enrich each reader branch with its inferred schema (ProviderOptions.ColumnTypes).
				// This allows --job to skip inference on subsequent runs.
				var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
				var optionsRegistry = _serviceProvider.GetRequiredService<OptionsRegistry>();
				jobs = await EnrichJobsWithSchemaAsync(jobs, dagDefinition, rootCommand, readerFactories, optionsRegistry, ct);

				if (jobs.Count > 1 || isDagYaml)
					JobFileWriter.Write(exportJobPath, jobs);
				else
					JobFileWriter.Write(exportJobPath, jobs.Values.First());

				return;
			}

			var resultsCollector = new System.Collections.Concurrent.ConcurrentQueue<DtPipe.Feedback.BranchSummary>();

		Func<ParseResult, CancellationToken, string[], JobDefinition?, string?, DtPipe.Core.Pipelines.Dag.BranchChannelContext?, Task<int>> executePipeline = async (pr, token, currentRawArgs, preParsedJob, localAlias, ctx) =>
			{
				// Perform preliminary contributor actions
				foreach (var contributor in _contributors)
				{
					var exitCode = await contributor.HandleCommandAsync(pr, token);
					if (exitCode.HasValue) return exitCode.Value;
				}

				// Check for deprecated options/prefixes
				CheckDeprecations(pr, _console, currentRawArgs);

				JobDefinition job;
				if (preParsedJob != null)
				{
					job = preParsedJob;
					var (argJobDict, _) = RawJobBuilder.Build(pr, cliJobOptions);
					var argJob = argJobDict.Values.First();
					job = job with {
						Input  = ctx?.ChannelInjection?.InputChannelAlias != null ? ChannelSpecHelper.ArrowMemory(ctx.ChannelInjection.InputChannelAlias) : argJob.Input,
						Output = ctx?.ChannelInjection?.OutputChannelAlias != null ? ChannelSpecHelper.ArrowMemory(ctx.ChannelInjection.OutputChannelAlias) : argJob.Output,
						Query  = !string.IsNullOrEmpty(argJob.Query) ? argJob.Query : job.Query
					};
				}
				else
				{
					var (jobsDict, jec) = RawJobBuilder.Build(pr, cliJobOptions);
					if (jec != 0) return jec;
					job = jobsDict.Values.First();
				}

				job = job with
				{
					Input = ResolveKeyring(job.Input, _console) ?? job.Input,
					Output = ResolveKeyring(job.Output, _console) ?? job.Output
				};

				var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();
				registry.BeginScope();

				var providerConfigService = new DtPipe.Cli.Services.ProviderConfigurationService(_contributors, registry);
				providerConfigService.BindOptions(job, pr);

				var linearPipelineService = new DtPipe.Cli.Services.LinearPipelineService(_contributors, _serviceProvider, registry, _console);
				return await linearPipelineService.ExecuteAsync(job, currentRawArgs, token, localAlias, dagDefinition.IsDag, ctx, resultsCollector, showStatusMessages: false);
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

			// 3. Validate Semantic Constraints (Singleton flags, DAG topology, etc.)
			var processorFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamTransformerFactory>>();
			var validationErrors = CliDagParser.Validate(dagDefinition, processorFactories);
			if (validationErrors.Count > 0)
			{
				foreach (var err in validationErrors)
					_console.MarkupLine($"[red]CLI Validation Error:[/] {err}");
				Environment.ExitCode = 1;
				return;
			}

			if (dagDefinition.IsDag)
			{
				// --dry-run on a DAG: run on a user-selected source branch
				if (RawJobBuilder.ParseDryRunFromArgs(rawArgs) > 0)
				{
					var sourceBranches = dagDefinition.Branches
						.Where(b => !string.IsNullOrEmpty(b.Input))
						.ToList();

					if (sourceBranches.Count == 0)
					{
						_console.MarkupLine("[yellow]⚠ No source branches available for dry-run.[/]");
						return;
					}

					DtPipe.Core.Pipelines.Dag.BranchDefinition selected;
					if (sourceBranches.Count == 1 || !_console.Profile.Capabilities.Interactive || Console.IsInputRedirected)
					{
						selected = sourceBranches[0];
						_console.MarkupLine($"[grey]Running dry-run on branch '[cyan]{selected.Alias}[/]'...[/]");
					}
					else
					{
						selected = _console.Prompt(
							new SelectionPrompt<DtPipe.Core.Pipelines.Dag.BranchDefinition>()
								.Title("[grey]Select a branch to dry-run:[/]")
								.UseConverter(b =>
								{
									var input = b.Input ?? "";
									var colonIdx = input.IndexOf(':');
									var provider = colonIdx > 0 ? input[..colonIdx] : input;
									return $"[cyan]{Markup.Escape(b.Alias)}[/]  [dim]({Markup.Escape(provider)})[/]";
								})
								.AddChoices(sourceBranches));
					}

					var branchPr = rootCommand.Parse(selected.Arguments);
					var dryRunExitCode = await executePipeline(branchPr, ct, selected.Arguments, selected.PreParsedJob, selected.Alias, null);
					if (dryRunExitCode != 0) Environment.ExitCode = dryRunExitCode;
					return;
				}

				_console.WriteLine();
				var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
				_console.Write(DagRenderer.BuildTopologyPanel(dagDefinition, readerFactories));
				_console.WriteLine();

				try
				{
					var orchestrator = _serviceProvider.GetRequiredService<IDagOrchestrator>();
					orchestrator.OnLogEvent = msg => _console.MarkupLine(msg);

					Func<DtPipe.Core.Pipelines.Dag.BranchDefinition, DtPipe.Core.Pipelines.Dag.BranchChannelContext, CancellationToken, Task<int>> branchExecutor = async (branch, ctx, token) =>
					{
						var branchPr = rootCommand.Parse(branch.Arguments);
						if (branchPr.Errors.Count > 0)
						{
							_console.Write(new Spectre.Console.Markup($"[red]Error parsing branch arguments:[/]{Environment.NewLine}"));
							foreach(var err in branchPr.Errors) _console.WriteLine(err.Message);
							return 1;
						}
						// Suppress log file setup on branches, they will inherit global settings or orchestrator will capture
						return await executePipeline(branchPr, token, branch.Arguments, branch.PreParsedJob, branch.Alias, ctx);
					};

					var exitCode = await orchestrator.ExecuteAsync(dagDefinition, branchExecutor, ct);
					_console.WriteLine();
					DagRenderer.PrintUnifiedResultsTable(resultsCollector.ToList(), dagDefinition, isDag: true, _console);
					
					if (exitCode != 0)
					{
						Environment.ExitCode = exitCode;
					}
					
					return;
				}
				catch (Exception ex)
				{
					_console.Write(new Spectre.Console.Markup($"{Environment.NewLine}[red]Error:[/] {Spectre.Console.Markup.Escape(ex.Message)}{Environment.NewLine}"));
					if (Environment.GetEnvironmentVariable("DEBUG") == "1")
					{
						_console.WriteException(ex);
					}
					Environment.ExitCode = 1;
					return;
				}
			}

			// Execution for non-DAG
			_console.WriteLine();
			var linearReaderFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
			_console.Write(DagRenderer.BuildLinearTopologyPanel(jobs.Values.First(), linearReaderFactories));
			_console.WriteLine();

			var finalExitCode = await executePipeline(parseResult, ct, rawArgs, jobs.Values.First(), null, null);
			if (finalExitCode == 0)
			{
				_console.WriteLine();
				DagRenderer.PrintUnifiedResultsTable(resultsCollector.ToList(), dagDefinition, isDag: false, _console);
			}
			else
			{
				Environment.ExitCode = finalExitCode;
			}
		});

		Action PrintHelpAction = () => PrintGroupedHelp(rootCommand, coreCoreOptions, _contributors, _console);
		return (rootCommand, PrintHelpAction, opts, allFlagPhases, _contributors.ToList());
	}

	private static void PrintGroupedHelp(RootCommand rootCommand, List<Option> coreOptions, IEnumerable<ICliContributor> contributors, IAnsiConsole console)
	{
		console.WriteLine("Description:");
		console.WriteLine($"  {rootCommand.Description}");
		console.WriteLine();
		console.WriteLine("Usage:");
		console.WriteLine("  dtpipe [options]");
		console.WriteLine();

		// Group core options by phase, using CoreFlagPhases as the authority.
		// Options with no phase entry (--drop, --rename, --throttle, etc.) fall to "Column Transformers".
		var phases = CoreOptionsBuilder.CoreFlagPhases;

		var globalOpts    = coreOptions.Where(o => phases.TryGetValue(o.Name, out var p) && p == CliPipelinePhase.Global).ToList();
		var readerOpts    = coreOptions.Where(o => phases.TryGetValue(o.Name, out var p) && p == CliPipelinePhase.Reader).ToList();
		var writerOpts    = coreOptions.Where(o => phases.TryGetValue(o.Name, out var p) && p == CliPipelinePhase.Writer).ToList();
		var transformerOpts = coreOptions.Where(o =>
		{
			if (!phases.TryGetValue(o.Name, out var p)) return true; // No phase entry → catch-all
			return p != CliPipelinePhase.Global && p != CliPipelinePhase.Reader && p != CliPipelinePhase.Writer;
		}).ToList();

		PrintSection("Global Pipeline Options", globalOpts, console);
		PrintSection("Data Operations", readerOpts, console);
		PrintSection("Column Transformers", transformerOpts, console);
		PrintSection("Target / Schema Options", writerOpts, console);

		// Track names already shown in core sections so contributors don't duplicate them
		var alreadyPrinted = new HashSet<string>(
			globalOpts.Concat(readerOpts).Concat(writerOpts).Concat(transformerOpts).Select(o => o.Name),
			StringComparer.OrdinalIgnoreCase);

		// Contributor options grouped by category
		var groups = contributors.GroupBy(c => c.Category).OrderBy(g => g.Key);
		foreach (var group in groups)
		{
			var optionsPrinted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			var groupOptions = new List<Option>();
			foreach (var contributor in group)
			{
				foreach (var opt in contributor.GetCliOptions())
				{
					if (!alreadyPrinted.Contains(opt.Name) &&
					    optionsPrinted.Add(opt.Name) &&
					    (opt.Description == null || !opt.Description.StartsWith("[HIDDEN]")))
						groupOptions.Add(opt);
				}
			}
			PrintSection(group.Key, groupOptions, console);
		}

		console.WriteLine("Other Options:");
		console.WriteLine("  -?, -h, --help                           Show this help");
		console.WriteLine("  --version                                Show version");
		console.WriteLine();

		console.WriteLine("Commands:");
		console.WriteLine("  inspect                                  Inspect the schema of a data source");
		console.WriteLine("  providers                                List all available data providers");
		console.WriteLine("  completion                               Generate or manage shell completion");
		console.WriteLine("  secret                                   Manage secure connection strings in OS Keyring");
		console.WriteLine();
	}

	private static void PrintSection(string title, List<Option> options, IAnsiConsole console)
	{
		if (options.Count == 0) return;
		console.WriteLine($"{title}:");
		foreach (var opt in options)
			PrintOption(opt, console);
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

	// ─────────────────────────────────────────────────────────────────────────

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

	private static void InjectStringProp(object obj, string propName, string? value)
	{
		if (string.IsNullOrEmpty(value)) return;
		var prop = obj.GetType().GetProperty(propName);
		if (prop != null && prop.CanWrite && prop.PropertyType == typeof(string)) prop.SetValue(obj, value);
	}
	private static void InjectBoolProp(object obj, string propName, bool value)
	{
		var prop = obj.GetType().GetProperty(propName);
		if (prop != null && prop.CanWrite && prop.PropertyType == typeof(bool)) prop.SetValue(obj, value);
	}
	private static void InjectIntProp(object obj, string propName, int value)
	{
		var prop = obj.GetType().GetProperty(propName);
		if (prop != null && prop.CanWrite && prop.PropertyType == typeof(int)) prop.SetValue(obj, value);
	}

	/// <summary>
	/// For each branch that has a file-based reader implementing <see cref="IColumnTypeInferenceCapable"/>,
	/// runs schema inference and embeds the result in <see cref="JobDefinition.ProviderOptions"/>.
	/// This enables <c>--job</c> to load the YAML and skip inference on subsequent runs.
	/// </summary>
	private static async Task<Dictionary<string, JobDefinition>> EnrichJobsWithSchemaAsync(
		Dictionary<string, JobDefinition> jobs,
		JobDagDefinition dagDefinition,
		RootCommand rootCommand,
		IEnumerable<IStreamReaderFactory> readerFactories,
		OptionsRegistry optionsRegistry,
		CancellationToken ct)
	{
		var result = new Dictionary<string, JobDefinition>(jobs);

		foreach (var branch in dagDefinition.Branches)
		{
			if (!jobs.TryGetValue(branch.Alias, out var job)) continue;
			if (string.IsNullOrEmpty(job.Input)) continue;

			var factory = readerFactories.FirstOrDefault(
				f => job.Input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase));
			if (factory == null) continue;

			try
			{
				// Re-parse branch args so per-branch options (e.g. --json-path) are correctly bound.
				var branchPr = rootCommand.Parse(branch.Arguments);
				var optType = factory.GetSupportedOptionTypes().FirstOrDefault();
				if (optType != null)
				{
					var instance = optionsRegistry.Get(optType);
					var cliOptions = (factory as ICliContributor)?.GetCliOptions() ?? System.Array.Empty<Option>();
					DtPipe.Cli.Infrastructure.CliOptionBuilder.BindForType(optType, instance, branchPr,
						cliOptions, isReaderScope: true);
					optionsRegistry.RegisterByType(optType, instance);
				}

				// Strip the provider prefix so factory.Create() resolves the correct file path.
				var cleanedInput = job.Input.StartsWith(factory.ComponentName + ":", StringComparison.OrdinalIgnoreCase)
					? job.Input.Substring(factory.ComponentName.Length + 1)
					: job.Input;
				optionsRegistry.Register(new PipelineOptions { ConnectionString = cleanedInput });

				// Inject universal reader options (path, column-types, encoding, …) from JobDefinition.
				// These are core options — not in the reader's own CLI options — so BindForType above
				// did not set them. MapProcessorProperties handles this injection by property name.
				if (optType != null)
				{
					var inst = optionsRegistry.Get(optType);
					InjectStringProp(inst, "Path",        job.Path);
					InjectStringProp(inst, "ColumnTypes", job.ColumnTypes);
					InjectStringProp(inst, "Encoding",    job.Encoding);
					if (job.AutoColumnTypes) InjectBoolProp(inst, "AutoColumnTypes", true);
					if (job.MaxSample > 0)   InjectIntProp(inst, "MaxSample", job.MaxSample);
					optionsRegistry.RegisterByType(optType, inst);
				}

				// Open the reader to run inference and capture the full Arrow schema.
				// This is safe for file-based readers (IColumnTypeInferenceCapable guard above).
				await using var reader = factory.Create(optionsRegistry);
				if (reader is not IColumnTypeInferenceCapable) continue;
				await reader.OpenAsync(ct);

				// Capture the complete Arrow schema (includes nested StructType, ListType, metadata).
				var arrowSchema = (reader as IColumnarStreamReader)?.Schema;
				if (arrowSchema is not { FieldsList: { Count: > 0 } }) continue;

				var schemaJson = DtPipe.Core.Infrastructure.Arrow.ArrowSchemaSerializer.SerializeCompact(arrowSchema);

				// Embed full Arrow schema directly in JobDefinition.Schema (per-branch).
				// Path is already in job.Path from branch arg parsing.
				// ProviderOptions keeps only genuinely provider-specific options (namespaces, separator, …).
				result[branch.Alias] = job with { Schema = schemaJson };
			}
			catch { /* best-effort: never block the export */ }
		}

		return result;
	}
}
