using System.CommandLine;
using DtPipe.Cli.Infrastructure;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
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

                        if (job.ProviderOptions != null)
                        {
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

                        if (job.ProviderOptions != null)
                        {
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
                        }

                        if (hasUpdates) _registry.RegisterByType(optionsType, instance);

                        // Also map top-level JobDefinition properties if they aren't already set
                        // This ensures YAML-defined properties (like Query, Main, Ref) reach the options object
                        MapXStreamerProperties(job, instance);
                    }
                }
            }
            // This new block handles CLI binding and then XStreamer property mapping for all IDataFactory types
            // including IXStreamerFactory, IDataWriterFactory, and IStreamReaderFactory.
            // It replaces the old `else if (contributor is IXStreamerFactory xFactory)` block.
            if (contributor is IDataFactory descriptor)
            {
                var options = contributor.GetCliOptions();
                var existingOptions = _registry.Get(descriptor.OptionsType);

                bool? isReaderScope = null;
                if (descriptor is IStreamReaderFactory) isReaderScope = true;
                else if (descriptor is IDataWriterFactory) isReaderScope = false;

                CliOptionBuilder.BindForType(descriptor.OptionsType, existingOptions, pr, options, isReaderScope);

                // Map XStreamer specific properties AFTER CLI binding to ensure they take precedence
                MapXStreamerProperties(job, existingOptions);

                _registry.RegisterByType(descriptor.OptionsType, existingOptions);
            }
        }

        // This second loop handles general CLI binding for all contributors and KeyAwareOptions
        foreach (var contributor in _contributors)
        {
            // This call is redundant if the new IDataFactory block above covers all contributors
            // that have CLI options. However, keeping it for now as per original structure.
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

    private void MapXStreamerProperties(JobDefinition job, object options)
    {
        var type = options.GetType();

        // Map Query
        if (!string.IsNullOrEmpty(job.Query))
        {
            var prop = type.GetProperty("Query");
            if (prop != null && prop.PropertyType == typeof(string) && prop.CanWrite)
            {
                var current = prop.GetValue(options) as string;
                if (string.IsNullOrEmpty(current)) prop.SetValue(options, job.Query);
            }
        }

        // Map MainAlias
        if (!string.IsNullOrEmpty(job.Main))
        {
            var prop = type.GetProperty("MainAlias");
            if (prop != null && prop.PropertyType == typeof(string) && prop.CanWrite)
            {
                var current = prop.GetValue(options) as string;
                if (string.IsNullOrEmpty(current)) prop.SetValue(options, job.Main);
            }
        }

        // Map RefAlias
        if (job.Ref != null && job.Ref.Length > 0)
        {
            var prop = type.GetProperty("RefAlias");
            if (prop != null && prop.PropertyType == typeof(string[]) && prop.CanWrite)
            {
                var current = prop.GetValue(options) as string[];
                if (current == null || current.Length == 0) prop.SetValue(options, job.Ref);
            }
        }
    }
}
