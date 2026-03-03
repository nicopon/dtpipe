using System.CommandLine;
using DtPipe.Cli.Infrastructure;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;


namespace DtPipe.Cli.Services;

/// <summary>
/// Responsible for merging and binding Job YAML settings and CLI arguments
/// into the Options objects registered in the OptionsRegistry.
/// </summary>
public class ProviderConfigurationService
{
    private readonly IEnumerable<ICliContributor> _contributors;
    private readonly OptionsRegistry _registry;

    public ProviderConfigurationService(IEnumerable<ICliContributor> contributors, OptionsRegistry registry)
    {
        _contributors = contributors;
        _registry = registry;
    }

    public void BindOptions(JobDefinition job, ParseResult pr)
    {
        if (job.ProviderOptions != null)
        {
            foreach (var contributor in _contributors)
            {
                if (contributor is IDataFactory factory && factory is IDataWriterFactory or IStreamReaderFactory)
                {
                    string providerName = factory.ComponentName;
                    if (contributor is IDataWriterFactory wFactory)
                    {
                        var optionsType = wFactory.GetSupportedOptionTypes().FirstOrDefault();
                        if (optionsType != null)
                        {
                            var instance = _registry.Get(optionsType);
                            bool hasUpdates = false;

                            if (job.ProviderOptions.TryGetValue(providerName, out var globalOpts))
                            {
                                ConfigurationBinder.Bind(instance, globalOpts);
                                hasUpdates = true;
                            }
                            if (job.ProviderOptions.TryGetValue($"{providerName}-writer", out var writerOpts))
                            {
                                ConfigurationBinder.Bind(instance, writerOpts);
                                hasUpdates = true;
                            }

                            if (hasUpdates)
                            {
                                if (!string.IsNullOrEmpty(job.Key) && instance is IKeyAwareOptions keyAware1)
                                    keyAware1.Key = job.Key;
                                _registry.RegisterByType(optionsType, instance);
                            }
                        }
                    }
                    else if (contributor is IStreamReaderFactory rFactory)
                    {
                        var optionsType = rFactory.GetSupportedOptionTypes().FirstOrDefault();
                        if (optionsType != null)
                        {
                            var instance = _registry.Get(optionsType);
                            bool hasUpdates = false;

                            if (job.ProviderOptions.TryGetValue(providerName, out var globalOpts))
                            {
                                ConfigurationBinder.Bind(instance, globalOpts);
                                hasUpdates = true;
                            }
                            if (job.ProviderOptions.TryGetValue($"{providerName}-reader", out var readerOpts))
                            {
                                ConfigurationBinder.Bind(instance, readerOpts);
                                hasUpdates = true;
                            }

                            if (hasUpdates) _registry.RegisterByType(optionsType, instance);
                        }
                    }
                }
            }
        }

        foreach (var contributor in _contributors)
        {
            contributor.BindOptions(pr, _registry);

            if (!string.IsNullOrEmpty(job.Key) && contributor is IDataWriterFactory wFactory)
            {
                var optionsType = wFactory.GetSupportedOptionTypes().FirstOrDefault();
                if (optionsType != null)
                {
                    var instance = _registry.Get(optionsType);
                    if (instance is IKeyAwareOptions keyAware2)
                    {
                        keyAware2.Key = job.Key;
                        _registry.RegisterByType(optionsType, instance);
                    }
                }
            }
        }
    }
}
