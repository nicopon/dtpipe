using System.CommandLine;
using DtPipe.Cli.Abstractions;
using DtPipe.Cli.Commands;
using DtPipe.Cli.Security;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
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

	public (RootCommand, Action) Build()
	{
		var inputOption = new Option<string?>("--input") { Description = "Input connection string or file path" };
		inputOption.Aliases.Add("-i");

		var queryOption = new Option<string?>("--query") { Description = "SQL query to execute (SELECT only)" };
		queryOption.Aliases.Add("-q");

		var outputOption = new Option<string?>("--output") { Description = "Output file path or connection string" };
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

		// Core Help Options
		var coreOptions = new List<Option> { inputOption, queryOption, outputOption, connectionTimeoutOption, queryTimeoutOption, batchSizeOption, unsafeQueryOption, dryRunOption, limitOption, samplingRateOption, samplingSeedOption, keyOption, jobOption, exportJobOption, logOption, preExecOption, postExecOption, onErrorExecOption, finallyExecOption, strategyOption, insertModeOption, tableOption, maxRetriesOption, retryDelayMsOption, strictSchemaOption, noSchemaValidationOption, metricsPathOption, autoMigrateOption };

		var rootCommand = new RootCommand("A simple, self-contained CLI for performance-focused data streaming & anonymization");
		foreach (var opt in coreOptions) rootCommand.Options.Add(opt);

		// Add Contributor Options
		foreach (var contributor in _contributors)
		{
			foreach (var opt in contributor.GetCliOptions())
			{
				if (!rootCommand.Options.Any(o => o.Name == opt.Name))
				{
					rootCommand.Options.Add(opt);
				}
			}
		}

		// Add Secret Command
		rootCommand.Subcommands.Add(new SecretCommand());

		rootCommand.SetAction(async (ParseResult parseResult, CancellationToken ct) =>
		{
			// Perform preliminary contributor actions

			foreach (var contributor in _contributors)
			{
				var exitCode = await contributor.HandleCommandAsync(parseResult, ct);
				if (exitCode.HasValue)
				{
					return exitCode.Value;
				}
			}

			// Check for deprecated options/prefixes
			CheckDeprecations(parseResult, _console);

			// Build Job Definition
			var (job, jobExitCode) = RawJobBuilder.Build(
				parseResult,
				jobOption, inputOption, queryOption, outputOption,
				connectionTimeoutOption, queryTimeoutOption, batchSizeOption,
				unsafeQueryOption, limitOption, samplingRateOption, samplingSeedOption, logOption, keyOption,
				preExecOption, postExecOption, onErrorExecOption, finallyExecOption, strategyOption, insertModeOption, tableOption,
				maxRetriesOption, retryDelayMsOption,
				strictSchemaOption,
				noSchemaValidationOption,
				metricsPathOption,
				autoMigrateOption);

			if (jobExitCode != 0)
			{
				return jobExitCode;
			}

			job = job with
			{
				Input = ResolveKeyring(job.Input, _console) ?? job.Input,
				Output = ResolveKeyring(job.Output, _console) ?? job.Output
			};

			// Export job
			var exportJobPath = parseResult.GetValue(exportJobOption);
			if (!string.IsNullOrWhiteSpace(exportJobPath))
			{
				var factoryList = _contributors.OfType<IDataTransformerFactory>().ToList();
				var configs = RawJobBuilder.BuildTransformerConfigsFromCli(
					Environment.GetCommandLineArgs(),
					factoryList,
					_contributors);

				job = job with { Transformers = configs };
				JobFileWriter.Write(exportJobPath, job);
				return 0;
			}

			var registry = _serviceProvider.GetRequiredService<OptionsRegistry>();

			// Hydrate from YAML ProviderOptions
			// This allows providers to expose custom options in YAML (e.g. "duck:Extension: httpfs")
			// matching them via reflection to their Option classes.
			if (job.ProviderOptions != null)
			{
				foreach (var contributor in _contributors)
				{
					if (contributor is IDataFactory factory && factory is IDataWriterFactory or IStreamReaderFactory)
					{
						string providerName = factory.ProviderName;

						if (contributor is IDataWriterFactory wFactory)
						{
							var optionsType = wFactory.GetSupportedOptionTypes().FirstOrDefault();
							if (optionsType != null)
							{
								var instance = registry.Get(optionsType);
								bool hasUpdates = false;

								// Apply global provider config (e.g., "csv")
								if (job.ProviderOptions.TryGetValue(providerName, out var globalOpts))
								{
									ConfigurationBinder.Bind(instance, globalOpts);
									hasUpdates = true;
								}

								// Apply writer-specific config (e.g., "csv-writer")
								if (job.ProviderOptions.TryGetValue($"{providerName}-writer", out var writerOpts))
								{
									ConfigurationBinder.Bind(instance, writerOpts);
									hasUpdates = true;
								}

								if (hasUpdates)
								{
									// Propagate Key via interface
									if (!string.IsNullOrEmpty(job.Key) && instance is IKeyAwareOptions keyAware1)
									{
										keyAware1.Key = job.Key;
									}
									registry.RegisterByType(optionsType, instance);
								}
							}
						}
						else if (contributor is IStreamReaderFactory rFactory)
						{
							var optionsType = rFactory.GetSupportedOptionTypes().FirstOrDefault();
							if (optionsType != null)
							{
								var instance = registry.Get(optionsType);
								bool hasUpdates = false;

								// Apply global provider config (e.g., "csv")
								if (job.ProviderOptions.TryGetValue(providerName, out var globalOpts))
								{
									ConfigurationBinder.Bind(instance, globalOpts);
									hasUpdates = true;
								}

								// Apply reader-specific config (e.g., "csv-reader")
								if (job.ProviderOptions.TryGetValue($"{providerName}-reader", out var readerOpts))
								{
									ConfigurationBinder.Bind(instance, readerOpts);
									hasUpdates = true;
								}

								if (hasUpdates)
								{
									registry.RegisterByType(optionsType, instance);
								}
							}
						}
					}
				}
			}

			// Bind Options
			foreach (var contributor in _contributors)
			{
				contributor.BindOptions(parseResult, registry);

				if (!string.IsNullOrEmpty(job.Key) && contributor is IDataWriterFactory wFactory)
				{
					var optionsType = wFactory.GetSupportedOptionTypes().FirstOrDefault();
					if (optionsType != null)
					{
						var instance = registry.Get(optionsType);
						// Propagate Key via interface — no reflection needed
						if (instance is IKeyAwareOptions keyAware2)
						{
							keyAware2.Key = job.Key;
							registry.RegisterByType(optionsType, instance);
						}
					}
				}
			}

			// Resolve Infrastructure
			var readerFactories = _contributors.OfType<IStreamReaderFactory>().ToList();
			var (readerFactory, cleanedInput) = ResolveFactory(readerFactories, job.Input, "reader");
			job = job with { Input = cleanedInput };

			_console.WriteLine($"Auto-detected input source: {readerFactory.ProviderName}");

			var writerFactories = _contributors.OfType<IDataWriterFactory>().ToList();
			var (writerFactory, cleanedOutput) = ResolveFactory(writerFactories, job.Output, "writer");
			job = job with { Output = cleanedOutput };

			job = job with
			{
				Query = LoadOrReadContent(job.Query, _console, "query"),
				PreExec = LoadOrReadContent(job.PreExec, _console, "Pre-Exec"),
				PostExec = LoadOrReadContent(job.PostExec, _console, "Post-Exec"),
				OnErrorExec = LoadOrReadContent(job.OnErrorExec, _console, "On-Error-Exec"),
				FinallyExec = LoadOrReadContent(job.FinallyExec, _console, "Finally-Exec")
			};

			if (readerFactory.RequiresQuery)
			{
				if (string.IsNullOrWhiteSpace(job.Query))
				{
					_console.Write(new Spectre.Console.Markup($"[red]Error: A query is required for provider '{readerFactory.ProviderName}'. Use --query \"SELECT...\"[/]{Environment.NewLine}"));
					return 1;
				}

				try
				{
					SqlQueryValidator.Validate(job.Query, job.UnsafeQuery);
				}
				catch (InvalidOperationException ex)
				{
					_console.Write(new Spectre.Console.Markup($"[red]Error: {ex.Message}[/]{Environment.NewLine}"));
					return 1;
				}
			}

			// Execution
			var options = new DumpOptions
			{
				Provider = readerFactory.ProviderName,
				ConnectionString = job.Input,
				Query = job.Query,
				OutputPath = job.Output,
				ConnectionTimeout = job.ConnectionTimeout,
				QueryTimeout = job.QueryTimeout,
				BatchSize = job.BatchSize,
				UnsafeQuery = job.UnsafeQuery,
				DryRunCount = RawJobBuilder.ParseDryRunFromArgs(Environment.GetCommandLineArgs()),
				Limit = job.Limit,
				SamplingRate = job.SamplingRate,
				SamplingSeed = job.SamplingSeed,
				LogPath = job.LogPath,
				Key = job.Key,
				PreExec = job.PreExec,
				PostExec = job.PostExec,
				OnErrorExec = job.OnErrorExec,
				FinallyExec = job.FinallyExec,
				Strategy = job.Strategy,
				InsertMode = job.InsertMode,
				Table = job.Table,
				MaxRetries = job.MaxRetries,
				RetryDelayMs = job.RetryDelayMs,
				StrictSchema = job.StrictSchema,
				NoSchemaValidation = job.NoSchemaValidation,
				MetricsPath = job.MetricsPath,
				AutoMigrate = job.AutoMigrate ?? false
			};

			registry.Register(options); // Register DumpOptions here, after it's declared

			if (!string.IsNullOrEmpty(options.LogPath))
			{
				Serilog.Log.Logger = new Serilog.LoggerConfiguration()
					.MinimumLevel.Debug()
					.WriteTo.File(options.LogPath)
					.CreateLogger();
				_loggerFactory.AddSerilog();
			}

			var exportService = _serviceProvider.GetRequiredService<ExportService>();
			var tFactories = _contributors.OfType<IDataTransformerFactory>().ToList();
			List<IDataTransformer> pipeline;

			if (job.Transformers != null && job.Transformers.Count > 0)
			{
				pipeline = BuildPipelineFromYaml(job.Transformers, tFactories, _console);
			}
			else
			{
				var pipelineBuilder = new TransformerPipelineBuilder(tFactories);
				pipeline = pipelineBuilder.Build(Environment.GetCommandLineArgs());
			}

			try
			{
				var pipelineOptions = new PipelineOptions
				{
					BatchSize      = options.BatchSize,
					Limit          = options.Limit,
					MaxRetries     = options.MaxRetries,
					RetryDelayMs   = options.RetryDelayMs,
					SamplingRate   = options.SamplingRate,
					SamplingSeed   = options.SamplingSeed,
					StrictSchema   = options.StrictSchema,
					NoSchemaValidation = options.NoSchemaValidation,
					AutoMigrate    = options.AutoMigrate,
					DryRunCount    = options.DryRunCount,
					PreExec        = options.PreExec,
					PostExec       = options.PostExec,
					OnErrorExec    = options.OnErrorExec,
					FinallyExec    = options.FinallyExec,
					NoStats        = options.NoStats,
					MetricsPath    = options.MetricsPath,
				};
				await exportService.RunExportAsync(pipelineOptions, options.Provider, options.OutputPath, ct, pipeline, readerFactory, writerFactory, registry);
				return 0;
			}
			catch (Exception ex)
			{
				_console.Write(new Spectre.Console.Markup($"{Environment.NewLine}[red]Error: {Markup.Escape(ex.Message)}[/]{Environment.NewLine}"));
				if (options.Provider == "duckdb" || Environment.GetEnvironmentVariable("DEBUG") == "1")
				{
					_console.WriteLine(ex.StackTrace ?? "");
				}
				return 1;
			}
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

	/// <summary>
	/// Builds a transformer pipeline from YAML TransformerConfig list.
	/// Matches each config's Type to factory's TransformerType and calls CreateFromYamlConfig.
	/// </summary>
	private static List<IDataTransformer> BuildPipelineFromYaml(
		List<TransformerConfig> configs,
		List<IDataTransformerFactory> factories,
		IAnsiConsole console)
	{
		var pipeline = new List<IDataTransformer>();

		foreach (var config in configs)
		{
			var factory = factories.FirstOrDefault(f =>
				f.TransformerType.Equals(config.Type, StringComparison.OrdinalIgnoreCase));

			if (factory == null)
			{
				console.Write(new Markup($"[yellow]Warning: Unknown transformer type '{config.Type}' in job file. Skipping.[/]{Environment.NewLine}"));
				continue;
			}

			var transformer = factory.CreateFromYamlConfig(config);
			if (transformer != null)
			{
				pipeline.Add(transformer);
			}
		}

		return pipeline;
	}


	/// <summary>
	/// Resolves the appropriate factory for a given connection string or file path.
	/// Supports deterministic resolution via "prefix:" and fallback to CanHandle().
	/// Returns the factory and the (potentially cleaned) connection string.
	/// </summary>
	private static (T Factory, string CleanedString) ResolveFactory<T>(IEnumerable<T> factories, string rawString, string typeName) where T : IDataFactory
	{
		rawString = rawString.Trim();

		// 1. Deterministic Prefix Check
		foreach (var factory in factories)
		{
			var prefix = factory.ProviderName + ":";

			// Check for "prefix:" behavior (standard)
			if (rawString.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
			{
				var cleaned = rawString.Substring(prefix.Length).Trim();
				return (factory, cleaned);
			}

			// Check for "prefix" behavior (e.g. "csv" or "parquet" for streams)
			// This allows syntax like: dtpipe -i csv -o parquet
			if (rawString.Equals(factory.ProviderName, StringComparison.OrdinalIgnoreCase))
			{
				return (factory, "");
			}
		}

		// 2. Fallback to CanHandle
		var detected = factories.FirstOrDefault(f => f.CanHandle(rawString));
		if (detected != null)
		{
			return (detected, rawString);
		}

		throw new InvalidOperationException($"Could not detect {typeName} provider for '{rawString}'. Please use a known prefix (e.g. 'duck:', 'ora:' ...) or file extension.");
	}

	private static bool IsPrefixMatch(Type optionsType, string configKey)
	{
		// Check "Prefix" static property
		var prefixProp = optionsType.GetProperty("Prefix", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
		if (prefixProp != null)
		{
			var prefix = prefixProp.GetValue(null) as string;
			if (string.Equals(prefix, configKey, StringComparison.OrdinalIgnoreCase)) return true;
		}
		return false;
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

	private static string? LoadOrReadContent(string? input, IAnsiConsole console, string contextName)
	{
		if (string.IsNullOrWhiteSpace(input)) return input;

		// Explicit @ syntax for forcing file read
		if (input.StartsWith('@'))
		{
			var path = input.Substring(1);
			if (File.Exists(path))
			{
				console.MarkupLine($"[grey]Loading {contextName} from file: {Markup.Escape(path)}[/]");
				return File.ReadAllText(path);
			}
			// Fallback: treat as literal string if file not found (e.g. SQL variable @foo)
			return input;
		}

		// Implicit file read if input matches an existing file path
		if (File.Exists(input))
		{
			console.MarkupLine($"[grey]Loading {contextName} from file: {Markup.Escape(input)}[/]");
			return File.ReadAllText(input);
		}

		return input;
	}

	private static void CheckDeprecations(ParseResult parseResult, IAnsiConsole console)
	{
		var args = Environment.GetCommandLineArgs();

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
