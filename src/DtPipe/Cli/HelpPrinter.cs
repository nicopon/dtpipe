using System.CommandLine;
using DtPipe.Core.Options;

namespace DtPipe.Cli;

/// <summary>
/// Custom help printer that organizes options by category.
/// </summary>
public static class HelpPrinter
{
	public static void PrintGroupedHelp(
		RootCommand rootCommand,
		IEnumerable<Option> coreOptions,
		Dictionary<Type, List<Option>> dynamicOptions,
		Option fakeOption,
		Option nullOption,
		Option fakeListOption)
	{
		Console.WriteLine("Description:");
		Console.WriteLine($"  {rootCommand.Description}");
		Console.WriteLine();
		Console.WriteLine("Usage:");
		Console.WriteLine("  dtpipe [options]");
		Console.WriteLine();

		// Core Options
		PrintSection("Core Options", coreOptions);

		// Group dynamic options by category
		var readers = new List<(string Name, List<Option> Options)>();
		var writers = new List<(string Name, List<Option> Options)>();
		var transformers = new List<(string Name, List<Option> Options)>();

		foreach (var (optionType, options) in dynamicOptions)
		{
			if (options.Count == 0) continue;

			var displayName = GetDisplayName(optionType);

			if (typeof(IProviderOptions).IsAssignableFrom(optionType))
			{
				readers.Add((displayName, options));
			}
			else if (typeof(IWriterOptions).IsAssignableFrom(optionType))
			{
				writers.Add((displayName, options));
			}
			else if (typeof(ITransformerOptions).IsAssignableFrom(optionType))
			{
				transformers.Add((displayName, options));
			}
		}

		// Print Reader options
		foreach (var (name, options) in readers)
		{
			PrintSection($"{name} Options", options);
		}

		// Print Writer options
		foreach (var (name, options) in writers)
		{
			PrintSection($"{name} Options", options);
		}

		// Print Anonymization options (custom options + auto-generated)
		var anonOptions = new List<Option> { fakeOption, nullOption, fakeListOption };
		foreach (var (name, options) in transformers)
		{
			anonOptions.AddRange(options);
		}
		PrintSection("Anonymization Options", anonOptions);

		// Standard options
		Console.WriteLine("Other Options:");
		Console.WriteLine("  -?, -h, --help     Show help and usage information");
		Console.WriteLine("  --version          Show version information");
		Console.WriteLine();
	}

	private static void PrintSection(string title, IEnumerable<Option> options)
	{
		var optionList = options.ToList();
		if (optionList.Count == 0) return;

		Console.WriteLine($"{title}:");
		foreach (var opt in optionList)
		{
			var aliases = string.Join(", ", opt.Aliases.OrderBy(a => a.Length));
			var desc = opt.Description ?? "";
			var defaultVal = GetDefaultValueString(opt);

			if (!string.IsNullOrEmpty(defaultVal))
			{
				desc += $" [default: {defaultVal}]";
			}

			Console.WriteLine($"  {aliases,-40} {desc}");
		}
		Console.WriteLine();
	}

	private static string GetDisplayName(Type optionType)
	{
		var prop = optionType.GetProperty("DisplayName",
			System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
		return prop?.GetValue(null)?.ToString() ?? optionType.Name.Replace("Options", "");
	}

	private static string GetDefaultValueString(Option option)
	{
		// Try to get default value through reflection
		try
		{
			var defaultFactory = option.GetType().GetProperty("DefaultValueFactory");
			if (defaultFactory?.GetValue(option) is Delegate factory)
			{
				var result = factory.DynamicInvoke(null);
				if (result is not null && result is not string { Length: 0 })
				{
					return result.ToString() ?? "";
				}
			}
		}
		catch { }
		return "";
	}
}
