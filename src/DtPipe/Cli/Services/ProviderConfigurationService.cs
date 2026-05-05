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

    public void BindOptions(JobDefinition job)
    {
        foreach (var contributor in _contributors)
        {
            if (contributor is IDataFactory factory)
            {
                var optionsType = factory.OptionsType;
                var instance = _registry.Get(optionsType);
                
                // 1. Bind from ProviderOptions (YAML/Globals)
                if (job.ProviderOptions != null)
                {
                    if (job.ProviderOptions.TryGetValue(factory.ComponentName, out var opts))
                        ConfigurationBinder.Bind(instance, opts);
                    
                    var suffix = (factory is IStreamReaderFactory) ? "-reader" : "-writer";
                    if (job.ProviderOptions.TryGetValue(factory.ComponentName + suffix, out var specificOpts))
                        ConfigurationBinder.Bind(instance, specificOpts);
                }

                // 2. Bind from RawArgs (CLI specific to this branch)
                // Reader contributors only receive args that appear BEFORE the first -o/--output
                // (positional scoping: flags after -o are writer-only).
                if (job.Arguments != null && job.Arguments.Length > 0)
                {
                    var argsToUse = job.Arguments;
                    if (factory is IStreamReaderFactory)
                    {
                        int outIdx = Array.FindIndex(job.Arguments, a =>
                            string.Equals(a, "-o", StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(a, "--output", StringComparison.OrdinalIgnoreCase));
                        if (outIdx > 0) argsToUse = job.Arguments[..outIdx];
                    }
                    var tempRegistry = new Pipeline.FlagRegistry();
                    foreach (var f in contributor.GetFlagDefs()) tempRegistry.Register(f);
                    Pipeline.FlagBinder.Bind(instance, argsToUse, tempRegistry, factory.ComponentName);
                }

                // 3. Map universal properties (Path, Query, etc.)
                MapProcessorProperties(job, instance);

                _registry.RegisterByType(optionsType, instance);
            }
        }

        PropagateKey(job.Key);
    }

    private void PropagateKey(string? key)
    {
        if (string.IsNullOrEmpty(key)) return;
        foreach (var contributor in _contributors.OfType<IDataWriterFactory>())
        {
            var optionsType = contributor.GetSupportedOptionTypes().FirstOrDefault();
            if (optionsType != null)
            {
                var instance = _registry.Get(optionsType);
                if (instance is IKeyAwareOptions keyAware)
                {
                    keyAware.Key = key;
                    _registry.RegisterByType(optionsType, instance);
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

        // Map universal reader/writer options — always override (JobDefinition is authoritative)
        MapString(type, options, "Path",        job.Path);
        MapString(type, options, "Table",       job.Table);
        MapString(type, options, "Strategy",    job.Strategy);
        MapString(type, options, "InsertMode",  job.InsertMode);
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

