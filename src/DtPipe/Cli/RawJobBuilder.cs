using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Cli.Abstractions;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;

namespace DtPipe.Cli;

/// <summary>
/// Responsible for parsing CLI arguments and building the initial JobDefinition.
/// Handles the precedence logic (CLI args > Job File > Defaults).
/// </summary>
public static class RawJobBuilder
{
	public static (JobDefinition Job, int ExitCode) Build(
		ParseResult parseResult,
		Option<string?> jobOption,
		Option<string?> inputOption,
		Option<string?> queryOption,
		Option<string?> outputOption,
		Option<int> connectionTimeoutOption,
		Option<int> queryTimeoutOption,
		Option<int> batchSizeOption,
		Option<bool> unsafeQueryOption,
		Option<int> limitOption,
		Option<double> samplingRateOption,
		Option<int?> samplingSeedOption,
		Option<string?> logOption,
		Option<string?> keyOption,
		Option<string?> preExecOption,
		Option<string?> postExecOption,
		Option<string?> onErrorExecOption,
		Option<string?> finallyExecOption,
		Option<string?> strategyOption,
		Option<string?> insertModeOption,
		Option<string?> tableOption,
		Option<int> maxRetriesOption,
		Option<int> retryDelayMsOption)
	{
		var jobFile = parseResult.GetValue(jobOption);
		JobDefinition job;

		if (!string.IsNullOrWhiteSpace(jobFile))
		{
			try
			{
				job = JobFileParser.Parse(jobFile);
				Console.Error.WriteLine($"Loaded job file: {jobFile}");

				// Apply CLI Overrides (Execution-time parameters only)
				var limitOverride = parseResult.GetValue(limitOption);
				if (limitOverride > 0)
				{
					job = job with { Limit = limitOverride };
				}

				var samplingRateOverride = parseResult.GetValue(samplingRateOption);
				if (samplingRateOverride is > 0 and < 1.0)
				{
					job = job with { SamplingRate = samplingRateOverride };
				}

				var samplingSeedOverride = parseResult.GetValue(samplingSeedOption);
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

				var logPathOverride = parseResult.GetValue(logOption);
				if (!string.IsNullOrEmpty(logPathOverride))
				{
					job = job with { LogPath = logPathOverride };
				}

				var keyOverride = parseResult.GetValue(keyOption);
				if (!string.IsNullOrEmpty(keyOverride))
				{
					job = job with { Key = keyOverride };
				}

				var preExecOverride = parseResult.GetValue(preExecOption);
				if (!string.IsNullOrEmpty(preExecOverride)) job = job with { PreExec = preExecOverride };

				var postExecOverride = parseResult.GetValue(postExecOption);
				if (!string.IsNullOrEmpty(postExecOverride)) job = job with { PostExec = postExecOverride };

				var onErrorExecOverride = parseResult.GetValue(onErrorExecOption);
				if (!string.IsNullOrEmpty(onErrorExecOverride)) job = job with { OnErrorExec = onErrorExecOverride };

				var finallyExecOverride = parseResult.GetValue(finallyExecOption);
				if (!string.IsNullOrEmpty(finallyExecOverride)) job = job with { FinallyExec = finallyExecOverride };

				var strategyOverride = parseResult.GetValue(strategyOption);
				if (!string.IsNullOrEmpty(strategyOverride)) job = job with { Strategy = strategyOverride };

				var insertModeOverride = parseResult.GetValue(insertModeOption);
				if (!string.IsNullOrEmpty(insertModeOverride)) job = job with { InsertMode = insertModeOverride };

				var tableOverride = parseResult.GetValue(tableOption);
				if (!string.IsNullOrEmpty(tableOverride)) job = job with { Table = tableOverride };

				var maxRetriesOverride = parseResult.GetValue(maxRetriesOption);
				if (maxRetriesOverride > 0) job = job with { MaxRetries = maxRetriesOverride };

				var retryDelayMsOverride = parseResult.GetValue(retryDelayMsOption);
				if (retryDelayMsOverride > 0) job = job with { RetryDelayMs = retryDelayMsOverride };
			}
			catch (Exception ex)
			{
				Console.Error.WriteLine($"Error loading job file: {ex.Message}");
				return (new JobDefinition { Input = "", Query = "", Output = "" }, 1);
			}
		}
		else
		{
			var query = parseResult.GetValue(queryOption);
			var output = parseResult.GetValue(outputOption);
			var input = parseResult.GetValue(inputOption);

			// Validation (Required args)
			if (string.IsNullOrWhiteSpace(output))
			{
				Console.Error.WriteLine("Error: Option '--output' is required (or use --job).");
				return (new JobDefinition { Input = "", Query = "", Output = "" }, 1);
			}

			if (string.IsNullOrWhiteSpace(input))
			{
				Console.Error.WriteLine("Error: --input is required.");
				return (new JobDefinition { Input = "", Query = "", Output = "" }, 1);
			}

			job = new JobDefinition
			{
				Input = input,
				Query = query,
				Output = output,
				ConnectionTimeout = parseResult.GetValue(connectionTimeoutOption),
				QueryTimeout = parseResult.GetValue(queryTimeoutOption),
				BatchSize = parseResult.GetValue(batchSizeOption),
				UnsafeQuery = parseResult.GetValue(unsafeQueryOption),
				DryRun = false, // Handled later for options
				Limit = parseResult.GetValue(limitOption),
				SamplingRate = parseResult.GetValue(samplingRateOption),
				SamplingSeed = parseResult.GetValue(samplingSeedOption),
				LogPath = parseResult.GetValue(logOption),
				Key = parseResult.GetValue(keyOption),
				PreExec = parseResult.GetValue(preExecOption),
				PostExec = parseResult.GetValue(postExecOption),
				OnErrorExec = parseResult.GetValue(onErrorExecOption),
				FinallyExec = parseResult.GetValue(finallyExecOption),
				Strategy = parseResult.GetValue(strategyOption),
				InsertMode = parseResult.GetValue(insertModeOption),
				Table = parseResult.GetValue(tableOption),
				MaxRetries = parseResult.GetValue(maxRetriesOption),
				RetryDelayMs = parseResult.GetValue(retryDelayMsOption)
			};
		}

		return (job, 0);
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
						Type = currentFactory.TransformerType,
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
					var factoryType = factory.TransformerType;

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
				Type = currentFactory.TransformerType,
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
