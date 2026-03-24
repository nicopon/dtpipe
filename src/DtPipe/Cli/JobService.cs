
using System.CommandLine;
using System.CommandLine.Parsing;
using System.CommandLine.Completions;
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
				MaxRetries = opts.MaxRetries,
				RetryDelayMs = opts.RetryDelayMs,
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
				SrcMain = opts.SrcMain,
				SrcRef = opts.SrcRef
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
							SqlQuery = !string.IsNullOrEmpty(kvp.Value.Sql) ? kvp.Value.Sql : null,
							FromAlias = kvp.Value.From,
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
							From = branch.FromAlias,
							Sql = branch.SqlQuery,
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
					// For DAG branches, the orchestrator might have injected -i and -o.
					// We must merge these into the pre-parsed job.
					var (argJobDict, _) = RawJobBuilder.Build(pr, cliJobOptions);
					var argJob = argJobDict.Values.First();
					job = job with { 
						Input = argJob.Input, 
						Output = argJob.Output,
						Query = !string.IsNullOrEmpty(argJob.Query) ? argJob.Query : job.Query
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
			var validationErrors = CliDagParser.Validate(dagDefinition);
			if (validationErrors.Count > 0)
			{
				foreach (var err in validationErrors)
					_console.MarkupLine($"[red]CLI Validation Error:[/] {err}");
				Environment.ExitCode = 1;
				return;
			}

			if (dagDefinition.IsDag)
			{
				_console.WriteLine();
				var readerFactories = _serviceProvider.GetRequiredService<IEnumerable<IStreamReaderFactory>>();
				_console.Write(BuildTopologyPanel(dagDefinition, readerFactories));
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

					await orchestrator.ExecuteAsync(dagDefinition, branchExecutor, ct);
					_console.WriteLine();
					PrintUnifiedResultsTable(resultsCollector.ToList(), dagDefinition, isDag: true, _console);
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
			_console.Write(BuildLinearTopologyPanel(jobs.Values.First(), linearReaderFactories));
			_console.WriteLine();

			var finalExitCode = await executePipeline(parseResult, ct, rawArgs, jobs.Values.First(), null, null);
			if (finalExitCode == 0)
			{
				_console.WriteLine();
				PrintUnifiedResultsTable(resultsCollector.ToList(), dagDefinition, isDag: false, _console);
			}
			else
			{
				Environment.ExitCode = finalExitCode;
			}
		});

		Action PrintHelpAction = () => PrintGroupedHelp(rootCommand, coreOptions, _contributors, _console);
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

		console.WriteLine("Core Options:");
		var basicCoreFlags = new HashSet<string> {
			"--input", "-i", "--output", "-o", "--query", "-q", "--job", "--sql", "--alias",
			"--dry-run", "--limit", "--batch-size", "-b", "--no-stats", "--log"
		};
		foreach (var opt in coreOptions.Where(o => basicCoreFlags.Contains(o.Name)))
		{
			PrintOption(opt, console);
		}
		console.WriteLine();

		var groups = contributors.GroupBy(c => c.Category).OrderBy(g => g.Key);

		foreach (var group in groups)
		{
			// Collect all options for this group
			var optionsPrinted = new HashSet<string>();
			var groupOptions = new List<Option>();

			foreach (var contributor in group)
			{
				foreach (var opt in contributor.GetCliOptions())
				{
					if (optionsPrinted.Add(opt.Name) && (opt.Description == null || !opt.Description.StartsWith("[HIDDEN]")))
					{
						groupOptions.Add(opt);
					}
				}
			}

			if (groupOptions.Any())
			{
				console.WriteLine($"{group.Key}:");
				foreach (var opt in groupOptions)
				{
					PrintOption(opt, console);
				}
				console.WriteLine();
			}
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

	// ─── Linear Topology Renderer ────────────────────────────────────────────

	private static Panel BuildLinearTopologyPanel(JobDefinition? job, IEnumerable<IStreamReaderFactory> readerFactories)
	{
		var sb = new System.Text.StringBuilder();
		string input = job?.Input ?? "";

		bool isColumnar = false;
		if (!string.IsNullOrEmpty(input))
		{
			var factory = readerFactories.FirstOrDefault(f =>
				input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase) ||
				input.Equals(f.ComponentName, StringComparison.OrdinalIgnoreCase) ||
				f.CanHandle(input));
			isColumnar = factory?.YieldsColumnarOutput == true;
		}

		string modeLabel = isColumnar ? "  [cyan]◈ Arrow[/]" : "  [yellow]● row[/]";
		string inputLabel = !string.IsNullOrEmpty(input) ? $"  [grey]{Markup.Escape(input)}[/]" : string.Empty;

		sb.AppendLine($"  [green]◉[/]{inputLabel}{modeLabel}");

		if (job?.Transformers != null)
			foreach (var t in job.Transformers)
				sb.AppendLine($"     [grey]→ {Markup.Escape(t.Type)}[/]");

		if (!string.IsNullOrEmpty(job?.Output))
			sb.AppendLine($"     [grey]──▶[/]  [blue]{Markup.Escape(job.Output)}[/]");

		return new Panel(new Markup(sb.ToString().TrimEnd()))
			.Header("[yellow] Pipeline [/]")
			.Border(BoxBorder.Rounded)
			.Padding(1, 0);
	}

	// ─── Unified Results Table ────────────────────────────────────────────────

	private static void PrintUnifiedResultsTable(
		List<DtPipe.Feedback.BranchSummary> results,
		JobDagDefinition dagDefinition,
		bool isDag,
		IAnsiConsole console)
	{
		if (results.Count == 0) return;

		if (isDag)
		{
			var branchOrder = dagDefinition.Branches
				.Select((b, i) => (b.Alias, Index: i))
				.ToDictionary(x => x.Alias, x => x.Index, StringComparer.OrdinalIgnoreCase);
			results = results
				.OrderBy(r => r.Alias != null && branchOrder.TryGetValue(r.Alias, out var idx) ? idx : int.MaxValue)
				.ToList();
		}

		bool hasMode = results.Any(r => r.TransformerModes.Count > 0 || r.ReaderIsColumnar);
		bool hasBranch = isDag && results.Count > 1;

		var table = new Table().Border(TableBorder.Rounded);
		table.Title = new TableTitle("[yellow] Results [/]");

		if (hasBranch) table.AddColumn(new TableColumn("[grey]Branch[/]"));
		table.AddColumn(new TableColumn("[grey]Stage[/]"));
		table.AddColumn(new TableColumn("[grey]Rows[/]").RightAligned());
		table.AddColumn(new TableColumn("[grey]Speed[/]").RightAligned());
		if (hasMode) table.AddColumn(new TableColumn("[grey]Mode[/]"));

		void AddRow(string branch, string stage, string rows, string speed, string mode)
		{
			var cols = new List<string>();
			if (hasBranch) cols.Add(branch);
			cols.Add(stage);
			cols.Add(rows);
			cols.Add(speed);
			if (hasMode) cols.Add(mode);
			table.AddRow(cols.ToArray());
		}

		long totalRows = 0;
		double peakMemory = 0;
		DateTime minStart = DateTime.MaxValue, maxEnd = DateTime.MinValue;
		bool firstBranch = true;

		foreach (var summary in results)
		{
			if (!firstBranch)
				AddRow("", "", "", "", "");
			firstBranch = false;

			var m = summary.Metrics;
			double elapsed = m.Duration.TotalSeconds;
			string branchLabel = summary.Alias != null ? $"[white][[{Markup.Escape(summary.Alias)}]][/]" : "";
			string readMode = summary.ReaderIsColumnar ? "[cyan]◈ Arrow[/]" : "[yellow]● row[/]";

			AddRow(branchLabel, "[grey]▸ Reading[/]", $"[white]{m.ReadCount:N0}[/]", $"[grey]{FormatSpeed(m.ReadCount, elapsed)}[/]", readMode);

			var indexedCounts = m.TransformerCountsByIndex;
			for (int ti = 0; ti < summary.TransformerModes.Count; ti++)
			{
				var (name, isColumnar) = summary.TransformerModes[ti];
				long count = indexedCounts != null && ti < indexedCounts.Count
					? indexedCounts[ti]
					: (m.TransformerStats.TryGetValue(name, out var c) ? c : 0);
				string modeLbl = isColumnar ? "[cyan]◈ columnar[/]" : "[yellow]● row[/]";
				AddRow("", $"[grey]▸ → {Markup.Escape(name)}[/]", $"[white]{count:N0}[/]", $"[grey]{FormatSpeed(count, elapsed)}[/]", modeLbl);
			}

			AddRow("", "[grey]▸ Writing[/]", $"[white]{m.WriteCount:N0}[/]", $"[grey]{FormatSpeed(m.WriteCount, elapsed)}[/]", "");

			totalRows += m.WriteCount;
			if (m.PeakMemoryWorkingSetMb > peakMemory) peakMemory = m.PeakMemoryWorkingSetMb;
			if (m.StartTime < minStart) minStart = m.StartTime;
			if (m.EndTime > maxEnd) maxEnd = m.EndTime;
		}

		console.Write(table);

		double totalElapsed = maxEnd > minStart ? (maxEnd - minStart).TotalSeconds : 0;
		string completionLine = isDag && results.Count > 1
			? $"[green]✓[/] [grey]Total[/]  [white]{totalRows:N0} rows[/] [grey]·  {totalElapsed:F1}s  ·  peak {peakMemory:F0} MB[/]"
			: $"[green]✓[/] [white]{totalRows:N0} rows[/] [grey]·  {totalElapsed:F1}s  ·  peak {peakMemory:F0} MB[/]";
		console.MarkupLine(completionLine);
	}

	private static string FormatSpeed(long count, double elapsedSeconds)
	{
		double rps = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
		return rps switch
		{
			>= 1_000_000 => $"{rps / 1_000_000:F1}M/s",
			>= 1_000 => $"{rps / 1_000:F1}K/s",
			_ => $"{rps:F0}/s"
		};
	}

	// ─── DAG Topology Renderer ───────────────────────────────────────────────

	private static Panel BuildTopologyPanel(JobDagDefinition dag, IEnumerable<IStreamReaderFactory> readerFactories)
	{
		var readerFactoryList = readerFactories.ToList();
		var lines = new System.Text.StringBuilder();
		bool first = true;

		foreach (var branch in dag.Branches)
		{
			if (!first) lines.AppendLine();
			first = false;

			if (branch.HasStreamTransformer)
			{
				// Processor branch: header shows alias + main source + refs side by side
				var fromPart = branch.FromAlias != null
					? $"  [grey]← [[{Markup.Escape(branch.FromAlias)}]][/]"
					: string.Empty;
				var refPart = branch.RefAliases.Any()
					? $"  [grey]+ref {string.Join(", ", branch.RefAliases.Select(r => $"[[{Markup.Escape(r)}]]"))}[/]"
					: string.Empty;
				var mergePart = branch.MergeAliases.Any()
					? $"  [grey]+merge {string.Join(", ", branch.MergeAliases.Select(m => $"[[{Markup.Escape(m)}]]"))}[/]"
					: string.Empty;

				lines.AppendLine($" [cyan]⚡[/] [white][[{Markup.Escape(branch.Alias)}]][/]{fromPart}{refPart}{mergePart}");

				// SQL/merge step
				if (branch.SqlQuery != null)
				{
					var sql = branch.SqlQuery.Trim().Replace('\n', ' ').Replace('\r', ' ');
					while (sql.Contains("  ")) sql = sql.Replace("  ", " ");
					if (sql.Length > 60) sql = sql[..57] + "...";
					lines.AppendLine($"      [grey]SQL › {Markup.Escape(sql)}[/]");
				}
				else
				{
					lines.AppendLine($"      [grey]merge[/]");
				}

				AppendTransformers(lines, branch, "      ");

				if (!string.IsNullOrEmpty(branch.Output))
					lines.AppendLine($"      [grey]──▶[/]  [blue]{Markup.Escape(branch.Output)}[/]");
			}
			else if (!string.IsNullOrEmpty(branch.FromAlias))
			{
				// Fan-out consumer: header shows relationship, steps follow
				lines.AppendLine($"  [green]◉[/] [white][[{Markup.Escape(branch.Alias)}]][/]  [grey]← [[{Markup.Escape(branch.FromAlias)}]][/]");
				AppendTransformers(lines, branch, "      ");
				if (!string.IsNullOrEmpty(branch.Output))
					lines.AppendLine($"      [grey]──▶[/]  [blue]{Markup.Escape(branch.Output)}[/]");
			}
			else
			{
				// Source branch: header shows alias + downstream, then steps
				bool isArrow = TopologyIsArrowChannel(dag, branch.Alias, readerFactoryList);
				bool feedsChannel = string.IsNullOrEmpty(branch.Output);

				// Downstream consumers (fan-out targets and SQL/merge processors consuming this alias)
				var consumers = dag.Branches
					.Where(b => b != branch && (
						(b.FromAlias != null && b.FromAlias.Equals(branch.Alias, StringComparison.OrdinalIgnoreCase)) ||
						b.RefAliases.Contains(branch.Alias, StringComparer.OrdinalIgnoreCase) ||
						b.MergeAliases.Contains(branch.Alias, StringComparer.OrdinalIgnoreCase)))
					.Select(b => $"[[{Markup.Escape(b.Alias)}]]")
					.ToList();

				string downstreamLabel = consumers.Count > 0 && feedsChannel
					? $"  [grey]→  {string.Join(", ", consumers)}[/]"
					: string.Empty;

				lines.AppendLine($"  [green]◉[/] [white][[{Markup.Escape(branch.Alias)}]][/]{downstreamLabel}");

				// Reader step
				if (!string.IsNullOrEmpty(branch.Input))
				{
					string modeLabel = feedsChannel
						? (isArrow ? "  [cyan]◈ Arrow[/]" : "  [yellow]● row[/]")
						: string.Empty;
					lines.AppendLine($"      [grey]← {Markup.Escape(branch.Input)}[/]{modeLabel}");
				}

				AppendTransformers(lines, branch, "      ");

				if (!string.IsNullOrEmpty(branch.Output))
					lines.AppendLine($"      [grey]──▶[/]  [blue]{Markup.Escape(branch.Output)}[/]");
			}
		}

		var content = lines.ToString().TrimEnd();
		return new Panel(new Markup(content))
			.Header("[yellow] Pipeline [/]")
			.Border(BoxBorder.Rounded)
			.Padding(1, 0);
	}
	private static bool TopologyIsArrowChannel(JobDagDefinition dag, string alias, List<IStreamReaderFactory> readerFactories)
	{
		// Arrow if consumed by a stream-transformer branch
		if (dag.Branches.Any(b => b.HasStreamTransformer && (
			(b.FromAlias != null && b.FromAlias.Equals(alias, StringComparison.OrdinalIgnoreCase)) ||
			b.RefAliases.Contains(alias, StringComparer.OrdinalIgnoreCase) ||
			b.MergeAliases.Contains(alias, StringComparer.OrdinalIgnoreCase))))
			return true;

		// Arrow if the producer reader yields columnar output (e.g. generate, parquet, Arrow)
		var producer = dag.Branches.FirstOrDefault(b =>
			b.Alias.Equals(alias, StringComparison.OrdinalIgnoreCase) && !b.HasStreamTransformer);
		if (producer != null && !string.IsNullOrEmpty(producer.Input))
		{
			var factory = readerFactories.FirstOrDefault(f =>
				producer.Input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase) ||
				producer.Input.Equals(f.ComponentName, StringComparison.OrdinalIgnoreCase) ||
				f.CanHandle(producer.Input));
			if (factory?.YieldsColumnarOutput == true) return true;
		}
		return false;
	}

	private static void AppendTransformers(System.Text.StringBuilder sb, BranchDefinition branch, string indent)
	{
		var transformers = branch.PreParsedJob?.Transformers;
		if (transformers?.Any() != true) return;
		foreach (var t in transformers)
			sb.AppendLine($"{indent}[grey]→ {Markup.Escape(t.Type)}[/]");
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
}
