using System.CommandLine;
using DtPipe.Cli.Abstractions;
using DtPipe.Core.Options;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class CliCollisionTests
{
	[Fact]
	public void DetectCollisions_ShouldHaveUniqueOptions()
	{
		// 1. Gather all implemented ICliContributors
		var assembly = typeof(ICliContributor).Assembly;
		var contributorTypes = assembly.GetTypes()
			.Where(t => typeof(ICliContributor).IsAssignableFrom(t) && !t.IsInterface && !t.IsAbstract && !t.ContainsGenericParameters)
			.Where(t => t.Name != "CliDataWriterFactory" && t.Name != "CliStreamReaderFactory") // Skip infra wrappers requiring complex deps
			.ToList();

		var optionsRegistry = new OptionsRegistry();

		// 2. Instantiate and get options
		var allOptions = new List<(string Source, Option Option)>();

		// Add Core Options manually (mirrors CliService build logic)
		// Note: If you add core options, update this list!
		var connOpt = new Option<string?>("--connection");
		connOpt.Aliases.Add("-c");

		var provOpt = new Option<string>("--provider");
		provOpt.Aliases.Add("-p");

		var queryOpt = new Option<string?>("--query");
		queryOpt.Aliases.Add("-q");

		var outputOpt = new Option<string?>("--output");
		outputOpt.Aliases.Add("-o");

		var batchOpt = new Option<int>("--batch-size");
		batchOpt.Aliases.Add("-b");

		var helpOpt = new Option<bool>("--help");
		helpOpt.Aliases.Add("-h");
		helpOpt.Aliases.Add("-?");

		var coreOptions = new List<Option>
		{
			connOpt,
			provOpt,
			queryOpt,
			outputOpt,
			new Option<int>("--connection-timeout"),
			new Option<int>("--query-timeout"),
			batchOpt,
			helpOpt,
			new Option<bool>("--version")
		};

		foreach (var opt in coreOptions) allOptions.Add(("Core", opt));

		foreach (var type in contributorTypes)
		{
			object instance;
			// Use reflection to find the constructor and inject known dependencies
			var ctor = type.GetConstructors().FirstOrDefault();
			if (ctor != null)
			{
				var args = new List<object?>();
				foreach (var param in ctor.GetParameters())
				{
					if (param.ParameterType == typeof(OptionsRegistry))
					{
						args.Add(optionsRegistry);
					}
					else if (param.ParameterType == typeof(DtPipe.Core.Services.IJsEngineProvider))
					{
						args.Add(Moq.Mock.Of<DtPipe.Core.Services.IJsEngineProvider>());
					}
					else if (param.ParameterType == typeof(Microsoft.Extensions.Logging.ILoggerFactory))
					{
						args.Add(new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory());
					}
					else
					{
						// Fallback for unknown optional params? For now assume null or try creating default
						args.Add(null);
					}
				}

				try
				{
					instance = ctor.Invoke(args.ToArray());
				}
				catch (Exception ex)
				{
					Console.Error.WriteLine($"FAILED to instantiate {type.FullName} via reflection.");
					throw new InvalidOperationException($"Could not instantiate contributor {type.Name}. Constructor parameters: {string.Join(", ", ctor.GetParameters().Select(p => p.ParameterType.Name))}", ex);
				}
			}
			else
			{
				// No public constructor? Try parameterless just in case
				try
				{
					instance = Activator.CreateInstance(type)!;
				}
				catch (Exception ex)
				{
					Console.Error.WriteLine($"FAILED to instantiate {type.FullName} (no public constructor found).");
					throw new InvalidOperationException($"Could not instantiate contributor {type.Name}.", ex);
				}
			}

			if (instance is ICliContributor contributor)
			{
				foreach (var opt in contributor.GetCliOptions())
				{
					allOptions.Add((type.Name, opt));
				}
			}
		}

		// 3. Check for duplicates in Names and Aliases
		var definedAliases = new Dictionary<string, List<string>>(); // Alias/Name -> List of Sources

		foreach (var (source, opt) in allOptions)
		{
			HashSet<string> namesToCheck = [opt.Name, .. opt.Aliases];

			foreach (var name in namesToCheck)
			{
				if (!definedAliases.TryGetValue(name, out var sources))
				{
					sources = [];
					definedAliases[name] = sources;
				}
				sources.Add(source);
			}
		}

		// 4. Trace collisions
		// We only care if the list of sources has > 1 DISTINCT elements.
		// (A source defining the same option name twice is also weird but System.CommandLine might handle it, 
		//  but sharing between different sources is a collision)
		var collisions = definedAliases
								.Where(kv => kv.Value.Distinct().Count() > 1)
								.Select(kv => $"Option '{kv.Key}' is claimed by multiple sources: {string.Join(", ", kv.Value.Distinct())}")
								.ToList();

		collisions.Should().BeEmpty("CLI options (names or aliases) must be unique across all contributors and core options.");
	}
}
