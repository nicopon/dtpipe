
using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli;
using DtPipe.Core.Pipelines;

namespace DtPipe.Cli;

/// <summary>
/// Responsible for parsing CLI arguments and building the initial JobDefinition.
/// Handles the precedence logic (CLI args > Job File > Defaults).
/// </summary>
public static class RawJobBuilder
{
	public static (Dictionary<string, JobDefinition> Jobs, int ExitCode) Build(
		ParseResult parseResult,
		CliJobOptions opts)
	{
		var jobFile = parseResult.GetValue(opts.Job);
		Dictionary<string, JobDefinition> jobs;

		if (!string.IsNullOrWhiteSpace(jobFile))
		{
			try
			{
				jobs = JobFileParser.Parse(jobFile);
				Console.Error.WriteLine($"Loaded job file: {jobFile}");

				// Apply CLI Overrides to ALL jobs in the DAG
				foreach (var alias in jobs.Keys.ToList())
				{
					jobs[alias] = ApplyCliOverrides(jobs[alias], parseResult, opts);
				}
                return (jobs, 0);
			}
			catch (Exception ex)
			{
				Console.Error.WriteLine($"Error loading job file: {ex.Message}");
				return (new Dictionary<string, JobDefinition>(), 1);
			}
		}
		else
		{
			var query = parseResult.GetValue(opts.Query)?.FirstOrDefault();
			var output = parseResult.GetValue(opts.Output)?.FirstOrDefault();
			var input = parseResult.GetValue(opts.Input)?.FirstOrDefault();

			// Validation (Required args)
			bool isDag = parseResult.Tokens.Any(t => t.Value == "--sql" || t.Value == "--alias" || t.Value == "--from" || t.Value == "--main" || t.Value == "--ref" || t.Value == "--src-main" || t.Value == "--src-ref");
			var exportJobResult = parseResult.GetValue(opts.ExportJob);
			if (string.IsNullOrWhiteSpace(output) && !isDag && string.IsNullOrWhiteSpace(exportJobResult))
			{
				Console.Error.WriteLine("Error: Option '--output' is required (or use --job).");
				return (new Dictionary<string, JobDefinition>(), 1);
			}

			var sql = parseResult.GetValue(opts.Sql)?.FirstOrDefault();
			var fromAliases = parseResult.GetValue(opts.From);
			var mainAliases = parseResult.GetValue(opts.Main);
			var refAliases = parseResult.GetValue(opts.Ref);
			
			// DEBUG
			if (Environment.GetEnvironmentVariable("DEBUG") == "1")
			{
				Console.Error.WriteLine($"[DEBUG] input: {input}, sql: {sql}, from: {fromAliases?.Length ?? 0}, main: {mainAliases?.Length ?? 0}, ref: {refAliases?.Length ?? 0}");
			}

			bool hasSource = (fromAliases != null && fromAliases.Length > 0) || 
							 (mainAliases != null && mainAliases.Length > 0) ||
							 (refAliases != null && refAliases.Length > 0);
			
			if (string.IsNullOrWhiteSpace(input) && string.IsNullOrWhiteSpace(sql) && !hasSource)
			{
				Console.Error.WriteLine($"Error: --input or --sql is required for branch focus. (Tokens: {string.Join(" ", parseResult.Tokens.Select(t => t.Value))})");
				return (new Dictionary<string, JobDefinition>(), 1);
			}

			var job = new JobDefinition
			{
				Input = input ?? "",
				Query = query,
				Output = output ?? "",
				ConnectionTimeout = (parseResult.GetValue(opts.ConnectionTimeout) is { Length: > 0 } ct ? ct[0] : 10),
				QueryTimeout = (parseResult.GetValue(opts.QueryTimeout) is { Length: > 0 } qt ? qt[0] : 0),
				BatchSize = (parseResult.GetValue(opts.BatchSize) is { Length: > 0 } bs ? bs[0] : 50_000),
				UnsafeQuery = parseResult.GetValue(opts.UnsafeQuery),
				DryRun = false, // Handled later for options
				Limit = (parseResult.GetValue(opts.Limit) is { Length: > 0 } lim ? lim[0] : 0),
				SamplingSeed = (parseResult.GetValue(opts.SamplingSeed) is { Length: > 0 } ss ? ss[0] : null),
				LogPath = parseResult.GetValue(opts.Log),
				Key = parseResult.GetValue(opts.Key),
				PreExec = parseResult.GetValue(opts.PreExec),
				PostExec = parseResult.GetValue(opts.PostExec),
				OnErrorExec = parseResult.GetValue(opts.OnErrorExec),
				FinallyExec = parseResult.GetValue(opts.FinallyExec),
				Strategy = parseResult.GetValue(opts.Strategy)?.FirstOrDefault(),
				InsertMode = parseResult.GetValue(opts.InsertMode)?.FirstOrDefault(),
				Table = parseResult.GetValue(opts.Table)?.FirstOrDefault(),
				MaxRetries = (parseResult.GetValue(opts.MaxRetries) is { Length: > 0 } mr ? mr[0] : 3),
				RetryDelayMs = (parseResult.GetValue(opts.RetryDelayMs) is { Length: > 0 } rd ? rd[0] : 1000),
				StrictSchema = (parseResult.GetValue(opts.StrictSchema) is { Length: > 0 } sso ? sso[0] : null) ?? false,
				NoSchemaValidation = (parseResult.GetValue(opts.NoSchemaValidation) is { Length: > 0 } nsvo ? nsvo[0] : null) ?? false,
				MetricsPath = parseResult.GetValue(opts.MetricsPath)?.FirstOrDefault(),
				AutoMigrate = (parseResult.GetValue(opts.AutoMigrate) is { Length: > 0 } amvo ? amvo[0] : null) ?? false,
				Throttle = (parseResult.GetValue(opts.Throttle) is { Length: > 0 } th ? th[0] : 0),
				IgnoreNulls = parseResult.GetValue(opts.IgnoreNulls),
				Prefix = parseResult.GetValue(opts.Prefix),
				Drop = parseResult.GetValue(opts.Drop) ?? Array.Empty<string>(),
				Rename = parseResult.GetValue(opts.Rename) ?? Array.Empty<string>(),
				Sql = sql,
				NoStats = parseResult.GetValue(opts.NoStats)
			};

			if (ParseDryRunFromArgs(Environment.GetCommandLineArgs()) > 0) job = job with { DryRun = true };

			return (new Dictionary<string, JobDefinition> { { "main", job } }, 0);
		}
	}

	public static JobDefinition ApplyCliOverrides(JobDefinition job, ParseResult parseResult, CliJobOptions opts)
	{
		// Apply CLI Overrides (Execution-time parameters only)
		var limitOverride = parseResult.GetValue(opts.Limit)?.FirstOrDefault();
		if (limitOverride > 0)
		{
			job = job with { Limit = limitOverride.Value };
		}

		var samplingRateOverride = parseResult.GetValue(opts.SamplingRate)?.FirstOrDefault();
		if (samplingRateOverride is > 0 and < 1.0)
		{
			job = job with { SamplingRate = samplingRateOverride.Value };
		}

		var samplingSeedOverride = parseResult.GetValue(opts.SamplingSeed) is { Length: > 0 } seedArr ? seedArr[0] : null;
		if (samplingSeedOverride.HasValue)
		{
			job = job with { SamplingSeed = samplingSeedOverride };
		}

		// Parse --dry-run (special handling for flag/value)
		var dryRunVal = ParseDryRunFromArgs(Environment.GetCommandLineArgs());
		if (dryRunVal > 0)
		{
			job = job with { DryRun = true };
		}

		var logPathOverride = parseResult.GetValue(opts.Log);
		if (!string.IsNullOrEmpty(logPathOverride))
		{
			job = job with { LogPath = logPathOverride };
		}

		var keyOverride = parseResult.GetValue(opts.Key);
		if (!string.IsNullOrEmpty(keyOverride))
		{
			job = job with { Key = keyOverride };
		}

		var preExecOverride = parseResult.GetValue(opts.PreExec);
		if (!string.IsNullOrEmpty(preExecOverride)) job = job with { PreExec = preExecOverride };

		var postExecOverride = parseResult.GetValue(opts.PostExec);
		if (!string.IsNullOrEmpty(postExecOverride)) job = job with { PostExec = postExecOverride };

		var onErrorExecOverride = parseResult.GetValue(opts.OnErrorExec);
		if (!string.IsNullOrEmpty(onErrorExecOverride)) job = job with { OnErrorExec = onErrorExecOverride };

		var finallyExecOverride = parseResult.GetValue(opts.FinallyExec);
		if (!string.IsNullOrEmpty(finallyExecOverride)) job = job with { FinallyExec = finallyExecOverride };

		var strategyOverride = parseResult.GetValue(opts.Strategy)?.FirstOrDefault();
		if (!string.IsNullOrEmpty(strategyOverride)) job = job with { Strategy = strategyOverride };

		var insertModeOverride = parseResult.GetValue(opts.InsertMode)?.FirstOrDefault();
		if (!string.IsNullOrEmpty(insertModeOverride)) job = job with { InsertMode = insertModeOverride };

		var tableOverride = parseResult.GetValue(opts.Table)?.FirstOrDefault();
		if (!string.IsNullOrEmpty(tableOverride)) job = job with { Table = tableOverride };

		var maxRetriesOverride = parseResult.GetValue(opts.MaxRetries);
		if (maxRetriesOverride is { Length: > 0 } mrvo && mrvo[0] > 0) job = job with { MaxRetries = mrvo[0] };

		var ssoOrig = parseResult.GetValue(opts.StrictSchema);
		if (ssoOrig is { Length: > 0 } ssov && ssov[0] is bool ssValue) job = job with { StrictSchema = ssValue };
 
		var nsvoOrig = parseResult.GetValue(opts.NoSchemaValidation);
		if (nsvoOrig is { Length: > 0 } nsvov && nsvov[0] is bool nsvValue) job = job with { NoSchemaValidation = nsvValue };
 
		var amvoOrig = parseResult.GetValue(opts.AutoMigrate);
		if (amvoOrig is { Length: > 0 } amvov && amvov[0] is bool amValue) job = job with { AutoMigrate = amValue };

		var metricsPathOverride = parseResult.GetValue(opts.MetricsPath)?.FirstOrDefault();
		if (!string.IsNullOrEmpty(metricsPathOverride)) job = job with { MetricsPath = metricsPathOverride };

		var prefixOverride = parseResult.GetValue(opts.Prefix);
		if (!string.IsNullOrEmpty(prefixOverride)) job = job with { Prefix = prefixOverride };

		var throttleOverride = parseResult.GetValue(opts.Throttle);
		if (throttleOverride is { Length: > 0 } thov && thov[0] > 0) job = job with { Throttle = thov[0] };

		var ignoreNullsOverride = parseResult.GetValue(opts.IgnoreNulls);
		if (ignoreNullsOverride) job = job with { IgnoreNulls = true };

		var dropOverride = parseResult.GetValue(opts.Drop);
		if (dropOverride?.Any() == true) job = job with { Drop = dropOverride };

		var renameOverride = parseResult.GetValue(opts.Rename);
		if (renameOverride?.Any() == true) job = job with { Rename = renameOverride };

		return job;
	}

	/// <summary>
	/// Builds TransformerConfig list from CLI args for YAML export.
	/// Uses logic similar to TransformerPipelineBuilder to group args by transformer type.
	/// </summary>
	public static List<TransformerConfig>? BuildTransformerConfigsFromCli(
		string[] args,
		List<IDataTransformerFactory> factories,
		IEnumerable<ICliContributor> contributors)
	{
		var configs = new List<TransformerConfig>();

		// Build option map (option alias -> (Factory, Option) tuple)
		var optionMap = new Dictionary<string, (IDataTransformerFactory Factory, Option Option)>(StringComparer.OrdinalIgnoreCase);
		foreach (var factory in factories)
		{
			if (factory is ICliContributor contributor)
			{
				foreach (var option in contributor.GetCliOptions())
				{
					if (!string.IsNullOrEmpty(option.Name))
						optionMap[option.Name] = (factory, option);
					foreach (var alias in option.Aliases)
						optionMap[alias] = (factory, option);
				}
			}
		}

		// Group consecutive args by transformer type
		IDataTransformerFactory? currentFactory = null;
		var currentMappings = new Dictionary<string, string>();
		var currentOptions = new Dictionary<string, string>();

		for (int i = 0; i < args.Length; i++)
		{
			var arg = args[i];

			if (optionMap.TryGetValue(arg, out var match))
			{
				var factory = match.Factory;
				var option = match.Option;

				if (factory != currentFactory && currentFactory != null && (currentMappings.Count > 0 || currentOptions.Count > 0))
				{
					configs.Add(new TransformerConfig
					{
						Type = currentFactory.ComponentName,
						Mappings = currentMappings.Count > 0 ? new Dictionary<string, string>(currentMappings) : null,
						Options = currentOptions.Count > 0 ? new Dictionary<string, string>(currentOptions) : null
					});
					currentMappings.Clear();
					currentOptions.Clear();
				}

				currentFactory = factory;

				// Determine if we should consume a value
				string? value = null;
				if (option.Arity.MaximumNumberOfValues > 0)
				{
					if (i + 1 < args.Length)
					{
						value = args[i + 1];
						i++;
					}
				}
				else
				{
					// Flag/Boolean option (Arity 0)
					value = "true";
				}

				if (value != null)
				{
					var optionName = arg.TrimStart('-');
					var factoryType = factory.ComponentName;

					if (optionName.Equals(factoryType, StringComparison.OrdinalIgnoreCase))
					{
						// Mapping (e.g., --fake "NAME:faker")
						var parts = value.Split(':', 2);
						if (parts.Length == 2)
						{
							currentMappings[parts[0].Trim()] = parts[1];
						}
						else
						{
							// Value-less mapping (e.g. --null "COL")
							currentMappings[value.Trim()] = "";
						}
					}
					else
					{
						// Config option (e.g., --fake-locale "fr")
						var optionKey = optionName;
						if (optionKey.StartsWith(factoryType + "-", StringComparison.OrdinalIgnoreCase))
						{
							optionKey = optionKey[(factoryType.Length + 1)..];
						}
						currentOptions[optionKey] = value;
					}
				}
			}
		}

		// Flush last
		if (currentFactory != null && (currentMappings.Count > 0 || currentOptions.Count > 0))
		{
			configs.Add(new TransformerConfig
			{
				Type = currentFactory.ComponentName,
				Mappings = currentMappings.Count > 0 ? new Dictionary<string, string>(currentMappings) : null,
				Options = currentOptions.Count > 0 ? new Dictionary<string, string>(currentOptions) : null
			});
		}

		return configs.Count > 0 ? configs : null;
	}

	/// <summary>
	/// Custom parser for --dry-run with optional int value.
	/// --dry-run alone = 1, --dry-run N = N, no flag = 0.
	/// </summary>
	public static int ParseDryRunFromArgs(string[] args)
	{
		for (int i = 0; i < args.Length; i++)
		{
			if (args[i] == "--dry-run")
			{
				// Check if next arg is a number
				if (i + 1 < args.Length && int.TryParse(args[i + 1], out var count) && count > 0)
				{
					return count;
				}
				// No number follows = default to 1 sample
				return 1;
			}
		}
		return 0;
	}
}
