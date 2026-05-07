
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
	public bool CanHandle(string connectionString)
	{
		if (connectionString.StartsWith(_descriptor.ComponentName + ":", StringComparison.OrdinalIgnoreCase)) return true;
		return _descriptor.CanHandle(connectionString);
	}
	public bool SupportsStdio => _descriptor.SupportsStdio;
	public Type OptionsType => _descriptor.OptionsType;

	// ICliContributor Implementation
	public string Category => _descriptor.Category;



	public IEnumerable<Pipeline.FlagDef> GetFlagDefs()
	{
		return CliOptionBuilder.GenerateFlagDefsForType(_descriptor.OptionsType);
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
		var route = registry.Get<ConnectionRoute>();

		// Validate that a target table was provided (FlagBinder set it from --table; YAML path via MapProcessorProperties)
		var tableProp = specificOptions.GetType().GetProperty("Table");
		if (tableProp != null)
		{
			var tableValue = tableProp.GetValue(specificOptions) as string;
			if (string.IsNullOrWhiteSpace(tableValue))
			{
				throw new InvalidOperationException($"A target table is required for provider '{_descriptor.ComponentName}'. Use --table \"[name]\"");
			}
		}

		return _descriptor.Create(route?.Output ?? "", specificOptions, _serviceProvider);
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
		var route = registry.Get<ConnectionRoute>();

		// Connection string is set by LinearPipelineService into ConnectionRoute after stripping the
		// component-name prefix. Query is set by FlagBinder (CLI path) or MapProcessorProperties (YAML path).
		return _descriptor.Create(route?.Input ?? "", specificOptions!, _serviceProvider);
	}

	public IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return _descriptor.OptionsType;
	}

	public bool RequiresQuery => _descriptor.RequiresQuery;

	public bool YieldsColumnarOutput => _descriptor.YieldsColumnarOutput;
}
