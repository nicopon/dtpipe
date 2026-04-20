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
                        MapProcessorProperties(job, instance);
                    }
                }
            }
            // This block handles CLI binding for all IDataFactory types
            // including IStreamTransformerFactory, IDataWriterFactory, and IStreamReaderFactory.
            if (contributor is IDataFactory descriptor)
            {
                var options = contributor.GetCliOptions();
                var existingOptions = _registry.Get(descriptor.OptionsType);

                bool? isReaderScope = null;
                if (descriptor is IStreamReaderFactory) isReaderScope = true;
                else if (descriptor is IDataWriterFactory) isReaderScope = false;

                CliOptionBuilder.BindForType(descriptor.OptionsType, existingOptions, pr, options, isReaderScope);

                // Map Processor specific properties AFTER CLI binding to ensure they take precedence
                MapProcessorProperties(job, existingOptions);

                _registry.RegisterByType(descriptor.OptionsType, existingOptions);
            }
        }

        // Propagate the --key option to all writer factories after CLI binding is complete
        if (!string.IsNullOrEmpty(job.Key))
        {
            foreach (var contributor in _contributors.OfType<IDataWriterFactory>())
            {
                var optionsType = contributor.GetSupportedOptionTypes().FirstOrDefault();
                if (optionsType != null)
                {
                    var instance = _registry.Get(optionsType);
                    if (instance is IKeyAwareOptions keyAware)
                    {
                        keyAware.Key = job.Key;
                        _registry.RegisterByType(optionsType, instance);
                    }
                }
            }
        }
    }

    private void MapProcessorProperties(JobDefinition job, object options)
    {
        var type = options.GetType();

        // Map Query (DB sources — only if not already set)
        if (!string.IsNullOrEmpty(job.Query))
        {
            var prop = type.GetProperty("Query");
            if (prop != null && prop.PropertyType == typeof(string) && prop.CanWrite)
            {
                var current = prop.GetValue(options) as string;
                if (string.IsNullOrEmpty(current)) prop.SetValue(options, job.Query);
            }
        }

        // Map RefAlias (SQL processors)
        if (job.Ref != null && job.Ref.Length > 0)
        {
            var prop = type.GetProperty("RefAlias");
            if (prop != null && prop.PropertyType == typeof(string[]) && prop.CanWrite)
            {
                var current = prop.GetValue(options) as string[];
                if (current == null || current.Length == 0) prop.SetValue(options, job.Ref);
            }
        }

        // Map universal reader options — always override (JobDefinition is authoritative)
        MapString(type, options, "Path",        job.Path);
        MapString(type, options, "ColumnTypes", job.ColumnTypes);
        MapString(type, options, "Encoding",    job.Encoding);
        MapString(type, options, "Schema",      job.Schema);

        if (job.AutoColumnTypes)
            MapBool(type, options, "AutoColumnTypes", true);

        if (job.MaxSample > 0)
            MapInt(type, options, "MaxSample", job.MaxSample);
    }

    private static void MapString(Type type, object options, string propName, string? value)
    {
        if (string.IsNullOrEmpty(value)) return;
        var prop = type.GetProperty(propName);
        if (prop != null && prop.CanWrite && prop.PropertyType == typeof(string))
            prop.SetValue(options, value);
    }

    private static void MapBool(Type type, object options, string propName, bool value)
    {
        var prop = type.GetProperty(propName);
        if (prop != null && prop.CanWrite && prop.PropertyType == typeof(bool))
            prop.SetValue(options, value);
    }

    private static void MapInt(Type type, object options, string propName, int value)
    {
        var prop = type.GetProperty(propName);
        if (prop != null && prop.CanWrite && prop.PropertyType == typeof(int))
            prop.SetValue(options, value);
    }
}

