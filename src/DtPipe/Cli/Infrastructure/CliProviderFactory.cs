using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;

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
	public string ComponentName => _descriptor.ComponentName;
	public bool CanHandle(string connectionString) => _descriptor.CanHandle(connectionString);
	public bool SupportsStdio => _descriptor.SupportsStdio;
	public Type OptionsType => _descriptor.OptionsType;

	// ICliContributor Implementation
	public string Category => _descriptor.Category;

	public IEnumerable<Option> GetCliOptions()
	{
		return _cliOptions ??= CliOptionBuilder.GenerateOptionsForType(_descriptor.OptionsType).ToList();
	}

	public string? BoundComponentName => _descriptor.ComponentName;

	protected virtual CliPipelinePhase DerivePhase()
	{
		if (typeof(TService) == typeof(IStreamReader)) return CliPipelinePhase.Reader;
		if (typeof(TService) == typeof(IDataWriter)) return CliPipelinePhase.Writer;
		return CliPipelinePhase.Global;
	}

	private IReadOnlyDictionary<string, CliPipelinePhase>? _flagPhases;
	public IReadOnlyDictionary<string, CliPipelinePhase> FlagPhases
	{
		get
		{
			if (_flagPhases == null)
			{
				var phases = new Dictionary<string, CliPipelinePhase>(StringComparer.OrdinalIgnoreCase);
				var phase = DerivePhase();
				foreach (var opt in GetCliOptions())
				{
					phases[opt.Name] = phase;
					foreach (var alias in opt.Aliases)
					{
						phases[alias] = phase;
					}
				}
				_flagPhases = phases;
			}
			return _flagPhases;
		}
	}

	public void BindOptions(ParseResult parseResult, OptionsRegistry registry)
	{
		var options = GetCliOptions();

		// Get existing options (from YAML default or generic default)
		var existingOptions = registry.Get(_descriptor.OptionsType);

		bool? isReaderScope = null;
		if (typeof(TService) == typeof(IStreamReader)) isReaderScope = true;
		else if (typeof(TService) == typeof(IDataWriter)) isReaderScope = false;

		// Apply CLI overrides on top
		CliOptionBuilder.BindForType(_descriptor.OptionsType, existingOptions, parseResult, options, isReaderScope);

		// Register/Update
		registry.RegisterByType(_descriptor.OptionsType, existingOptions);
	}
}

public class CliDataWriterFactory : CliProviderFactory<IDataWriter>, IDataWriterFactory
{
	public CliDataWriterFactory(IProviderDescriptor<IDataWriter> descriptor, OptionsRegistry registry, IServiceProvider serviceProvider)
		: base(descriptor, registry, serviceProvider)
	{
	}

	public IDataWriter Create(OptionsRegistry registry)
	{
		var specificOptions = registry.Get(_descriptor.OptionsType);
		var pipelineOptions = registry.Get<PipelineOptions>();

		// 1. Resolve Strategy
		ResolveGenericOption(specificOptions, "Strategy", pipelineOptions.Strategy, _descriptor.ComponentName);

		// 2. Resolve InsertMode
		ResolveGenericOption(specificOptions, "InsertMode", pipelineOptions.InsertMode, _descriptor.ComponentName);

		// 3. Resolve Table
		ResolveGenericOption(specificOptions, "Table", pipelineOptions.Table, _descriptor.ComponentName);

		var tableProp = specificOptions.GetType().GetProperty("Table");
		if (tableProp != null)
		{
			var tableValue = tableProp.GetValue(specificOptions) as string;
			if (string.IsNullOrWhiteSpace(tableValue))
			{
				throw new InvalidOperationException($"A target table is required for provider '{_descriptor.ComponentName}'. Use --table \"[name]\"");
			}
		}

		return _descriptor.Create(pipelineOptions.OutputPath, specificOptions, _serviceProvider);
	}

	private void ResolveGenericOption(object specificOptions, string propertyName, string? genericValue, string providerName)
	{
		var prop = specificOptions.GetType().GetProperty(propertyName);
		if (prop == null) return;

		var currentValue = prop.GetValue(specificOptions);

		try
		{
			var defaultInstance = Activator.CreateInstance(specificOptions.GetType());
			var defaultValue = prop.GetValue(defaultInstance);
			if (!Equals(currentValue, defaultValue)) return;
		}
		catch
		{
			if (currentValue != null) return;
		}

		var enumType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

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
			if (prop.GetValue(specificOptions) == null && enumType.IsValueType)
			{
				var defaultValue = Activator.CreateInstance(enumType);
				prop.SetValue(specificOptions, defaultValue);
			}
		}
	}

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

	public IStreamReader Create(OptionsRegistry registry)
	{
		var specificOptions = registry.Get(_descriptor.OptionsType);
		var pipelineOptions = registry.Get<PipelineOptions>();

		if (specificOptions is IQueryAwareOptions queryAware && !string.IsNullOrWhiteSpace(pipelineOptions.Query))
		{
			queryAware.Query = pipelineOptions.Query;
		}

		return _descriptor.Create(pipelineOptions.ConnectionString, specificOptions!, _serviceProvider);
	}

	public IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return _descriptor.OptionsType;
	}

	public bool RequiresQuery => _descriptor.RequiresQuery;

	public bool YieldsColumnarOutput => _descriptor.YieldsColumnarOutput;
}

public class CliProcessorFactory : CliProviderFactory<IStreamReader>, IXStreamerFactory, IStreamReaderFactory
{
    private readonly IXStreamerFactory _processorDescriptor;

    public CliProcessorFactory(IXStreamerFactory descriptor, OptionsRegistry registry, IServiceProvider serviceProvider)
        : base(descriptor, registry, serviceProvider)
    {
        _processorDescriptor = descriptor;
    }

    public ChannelMode ChannelMode => _processorDescriptor.ChannelMode;

    public bool RequiresQuery => _processorDescriptor.RequiresQuery;

    protected override CliPipelinePhase DerivePhase() => CliPipelinePhase.Processor;

    public IStreamReader Create(OptionsRegistry registry)
    {
        var specificOptions = registry.Get(_descriptor.OptionsType);
        var pipelineOptions = registry.Get<PipelineOptions>();

        return _processorDescriptor.Create(pipelineOptions.ConnectionString, specificOptions!, _serviceProvider);
    }

    public IStreamReader Create(string connectionString, object options, IServiceProvider serviceProvider)
    {
        // If 'options' is PipelineOptions (global), we MUST resolve the specific options
        // from the registry to get the mapped YAML properties or CLI overrides.
        if (options is PipelineOptions)
        {
            options = _registry.Get(_descriptor.OptionsType);
        }
        return _processorDescriptor.Create(connectionString, options, serviceProvider);
    }

    public IEnumerable<Type> GetSupportedOptionTypes()
    {
        yield return _descriptor.OptionsType;
    }
}
