using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Cli.Abstractions;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// Generic adapter that bridges a pure <see cref="IProviderDescriptor{TService}"/> to the CLI infrastructure.
/// Acts as both a CLI Contributor (exposing options) and a Data Factory (creating services).
/// </summary>
public class CliProviderFactory<TService> : ICliContributor, IDataFactory
{
	protected readonly IProviderDescriptor<TService> _descriptor;
	protected readonly OptionsRegistry _registry;
	protected readonly IServiceProvider _serviceProvider;
	private IEnumerable<Option>? _cliOptions;

	public CliProviderFactory(
		IProviderDescriptor<TService> descriptor,
		OptionsRegistry registry,
		IServiceProvider serviceProvider)
	{
		_descriptor = descriptor;
		_registry = registry;
		_serviceProvider = serviceProvider;
	}

	// IDataFactory Implementation
	public string ProviderName => _descriptor.ProviderName;
	public bool CanHandle(string connectionString) => _descriptor.CanHandle(connectionString);

	// ICliContributor Implementation
	// Determine category based on TService type
	public string Category => typeof(TService).Name switch
	{
		nameof(IDataWriter) => "Writer Options",
		nameof(IStreamReader) => "Reader Options",
		nameof(IDataTransformer) => "Transformer Options",
		_ => "Provider Options"
	};

	public IEnumerable<Option> GetCliOptions()
	{
		return _cliOptions ??= CliOptionBuilder.GenerateOptionsForType(_descriptor.OptionsType).ToList();
	}

	public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
	{
		var options = GetCliOptions();

		// Get existing options (from YAML default or generic default)
		var existingOptions = registry.Get(_descriptor.OptionsType);

		// Apply CLI overrides on top
		CliOptionBuilder.BindForType(_descriptor.OptionsType, existingOptions, parseResult, options);

		// Register/Update
		registry.RegisterByType(_descriptor.OptionsType, existingOptions);
	}

	// Factory method to actually produce the service
}

public class CliDataWriterFactory : CliProviderFactory<IDataWriter>, IDataWriterFactory
{
	public CliDataWriterFactory(IProviderDescriptor<IDataWriter> descriptor, OptionsRegistry registry, IServiceProvider serviceProvider)
		: base(descriptor, registry, serviceProvider)
	{
	}

	public IDataWriter Create(DumpOptions options)
	{
		// Resolve the specific options object from registry
		var specificOptions = _registry.Get(_descriptor.OptionsType);

		// 1. Resolve Strategy
		ResolveGenericOption(specificOptions, "Strategy", options.Strategy, _descriptor.ProviderName);

		// 2. Resolve InsertMode
		ResolveGenericOption(specificOptions, "InsertMode", options.InsertMode, _descriptor.ProviderName);

		// 3. Resolve Table
		ResolveGenericOption(specificOptions, "Table", options.Table, _descriptor.ProviderName);

		// Use the descriptor to create. 
		return _descriptor.Create(options.OutputPath, specificOptions, options, _serviceProvider);
	}

	private void ResolveGenericOption(object specificOptions, string propertyName, string? genericValue, string providerName)
	{
		var prop = specificOptions.GetType().GetProperty(propertyName);
		if (prop == null) return; // This provider doesn't support this option (e.g. SQLite has no InsertMode)

		// Check if specific option is already set to a non-default value
		var currentValue = prop.GetValue(specificOptions);

		try
		{
			var defaultInstance = Activator.CreateInstance(specificOptions.GetType());
			var defaultValue = prop.GetValue(defaultInstance);

			// If current value differs from default, assume user set it specifically -> Precedence to specific
			if (!Equals(currentValue, defaultValue)) return;
		}
		catch
		{
			// Ignore error creates default, just proceed if standard checks fail
			if (currentValue != null) return;
		}

		var enumType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

		// If generic option is provided, try to parse and set it
		if (!string.IsNullOrEmpty(genericValue))
		{
			if (enumType == typeof(string))
			{
				prop.SetValue(specificOptions, genericValue);
				return;
			}

			if (enumType.IsEnum)
			{
				try
				{
					var parsedValue = Enum.Parse(enumType, genericValue, ignoreCase: true);
					prop.SetValue(specificOptions, parsedValue);
				}
				catch (ArgumentException)
				{
					var allowed = string.Join(", ", Enum.GetNames(enumType));
					throw new InvalidOperationException($"Invalid {propertyName} '{genericValue}' for provider '{providerName}'. Allowed values: {allowed}");
				}
			}
		}
		else
		{
			// If still null, set to default (0-value of enum).
			if (prop.GetValue(specificOptions) == null && enumType.IsValueType)
			{
				// Instantiate default value (usually 0 = Append / Standard)
				var defaultValue = Activator.CreateInstance(enumType);
				prop.SetValue(specificOptions, defaultValue);
			}
		}
	}

	// IEnumerable<Type> IDataWriterFactory.GetSupportedOptionTypes()
	public IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return _descriptor.OptionsType;
	}
}

public class CliStreamReaderFactory : CliProviderFactory<IStreamReader>, IStreamReaderFactory
{
	public CliStreamReaderFactory(IProviderDescriptor<IStreamReader> descriptor, OptionsRegistry registry, IServiceProvider serviceProvider)
		: base(descriptor, registry, serviceProvider)
	{
	}

	public IStreamReader Create(DumpOptions options)
	{
		var specificOptions = _registry.Get(_descriptor.OptionsType);
		return _descriptor.Create(options.ConnectionString, specificOptions, options, _serviceProvider);
	}

	public IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return _descriptor.OptionsType;
	}

	public bool RequiresQuery => _descriptor.RequiresQuery;
}
