using System.CommandLine;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Cli.Abstractions;
using QueryDump.Configuration;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Options;

namespace QueryDump.Cli.Infrastructure;

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
    public string Category => typeof(TService).Name switch {
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
        var boundOptions = CliOptionBuilder.BindForType(_descriptor.OptionsType, parseResult, options);
        registry.RegisterByType(_descriptor.OptionsType, boundOptions);
    }
    
    // Factory method to actually produce the service
    // Note: We need specific interfaces like IDataWriterFactory to expose a strongly typed Create(DumpOptions)
    // This class serves as the base for those specific adapters or we can implement them explicitly if we change the architecture slightly.
    // However, CliService expects IDataWriterFactory, which has Create(DumpOptions).
    // So we need specific wrappers for Writer/Reader, or make this class implement them dynamically.
    // Simpler approach: Specific Generic Wrappers.
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
        
        // Use the descriptor to create. 
        // Note: We assume options.OutputPath is the connection string for Writers.
        // The descriptor expects "connectionString" and "optionsObject".
        // Now also passing full DumpOptions context.
        return _descriptor.Create(options.OutputPath, specificOptions, options, _serviceProvider);
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
}
