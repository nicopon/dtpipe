
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
		if (ComponentSelector.Matches(connectionString, _descriptor.ComponentName)) return true;
		return _descriptor.CanHandle(connectionString);
	}

	/// <summary>
	/// Hands the selector's "+{variant}" qualifier to options that declare they need it, so the
	/// provider never has to re-parse a prefix the router already removed.
	/// </summary>
	protected static void ApplyVariant(object? options, string? variant)
	{
		if (options is IVariantAwareOptions variantAware)
		{
			variantAware.Variant = variant;
		}
	}
	public bool SupportsStdio => _descriptor.SupportsStdio;
	public Type OptionsType => _descriptor.OptionsType;

	/// <summary>
	/// F5: forwards the internal-channel capability of the wrapped descriptor, if any,
	/// so typed endpoint routing can pick this factory without adapter-identity strings.
	/// </summary>
	public InternalChannelKind? CapabilityKind => (_descriptor as IInternalChannelCapable)?.ChannelKind;

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

		// Validate that a target table was provided (OptionBinder set it from --table; YAML path via provider-options).
		// F13: typed capability instead of GetProperty("Table") reflection.
		if (specificOptions is ITableAwareOptions tableOpts && string.IsNullOrWhiteSpace(tableOpts.Table))
		{
			// Both spellings, because the caller may have no command line: a model driving the MCP
			// server writes a YAML job, and a message naming only --table sends it after a flag it
			// cannot pass.
			throw new InvalidOperationException(
				$"A target table is required for provider '{_descriptor.ComponentName}'. "
				+ $"In a YAML job, set 'table' under provider-options -> {_descriptor.ComponentName}-writer; "
				+ "on the command line, pass --table \"<name>\".");
		}

		ApplyVariant(specificOptions, route?.OutputVariant);

		return _descriptor.Create(route?.Output ?? "", specificOptions, _serviceProvider);
	}

	public IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return _descriptor.OptionsType;
	}
}

public class CliStreamReaderFactory : CliProviderFactory<IStreamReader>, IStreamReaderFactory, IHasSqlDialect
{
	public ISqlDialect? Dialect => (_descriptor as IHasSqlDialect)?.Dialect;

	public CliStreamReaderFactory(IProviderDescriptor<IStreamReader> descriptor, OptionsRegistry registry, IServiceProvider serviceProvider)
		: base(descriptor, registry, serviceProvider)
	{
	}

	public IStreamReader Create(OptionsRegistry registry)
	{
		var specificOptions = registry.Get(_descriptor.OptionsType);

		// RequiresQuery providers must have a non-empty Query by now: LinearPipelineService's
		// RequiresQuery auto-build (--table fallback) already had its chance, and InspectCommand/MCP
		// validate upfront too. A still-empty Query here means neither ran or both failed silently —
		// fail loudly instead of letting the provider fall back to a default query of its own.
		if (_descriptor.RequiresQuery && specificOptions is IQueryAwareOptions queryAware && string.IsNullOrWhiteSpace(queryAware.Query))
		{
			throw new InvalidOperationException($"A query is required for provider '{ComponentName}'. Use --query \"SELECT ...\" or --table <name>.");
		}

		var route = registry.Get<ConnectionRoute>();

		// Connection string is set by LinearPipelineService into ConnectionRoute after ComponentSelector
		// stripped the selector. Query is set by OptionBinder (CLI path) or MapProcessorProperties (YAML path).
		ApplyVariant(specificOptions, route?.InputVariant);

		var reader = _descriptor.Create(route?.Input ?? "", specificOptions!, _serviceProvider);
		if (reader is IBatchSizeConfigurable batchConfigurable)
		{
			var pipelineOptions = registry.Get<PipelineOptions>();
			batchConfigurable.BatchSize = pipelineOptions.BatchSize;
			batchConfigurable.MaxBatchBytes = pipelineOptions.MaxBatchBytes;
		}
		return reader;
	}

	public IEnumerable<Type> GetSupportedOptionTypes()
	{
		yield return _descriptor.OptionsType;
	}

	public bool RequiresQuery => _descriptor.RequiresQuery;

	public bool YieldsColumnarOutput => _descriptor.YieldsColumnarOutput;
}
