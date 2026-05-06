using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Options;


namespace DtPipe.Cli.Services;

/// <summary>
/// Responsible for merging and binding Job settings and CLI arguments
/// into the Options objects registered in the OptionsRegistry.
///
/// Two binding paths:
/// - CLI path: FlagBinder reads adapter-specific flags directly from RawArgs (--query, --table,
///   --strict-schema, --key, --pre-exec, etc.). JobDefinition fields are null/default.
/// - YAML path: MapProcessorProperties copies non-null JobDefinition fields to adapter options.
///   FlagBinder has no RawArgs to process (job.Arguments is empty).
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

    public void BindOptions(JobDefinition job, GlobalOptions? globals = null)
    {
        foreach (var contributor in _contributors)
        {
            if (contributor is IDataFactory factory)
            {
                var optionsType = factory.OptionsType;
                var instance = _registry.Get(optionsType);
                bool isWriter = factory is IDataWriterFactory;

                // 1. Bind from ProviderOptions (YAML/Globals)
                if (job.ProviderOptions != null)
                {
                    if (job.ProviderOptions.TryGetValue(factory.ComponentName, out var opts))
                        ConfigurationBinder.Bind(instance, opts);

                    var suffix = isWriter ? "-writer" : "-reader";
                    if (job.ProviderOptions.TryGetValue(factory.ComponentName + suffix, out var specificOpts))
                        ConfigurationBinder.Bind(instance, specificOpts);
                }

                // 2. Bind from stage-scoped args (CLI path).
                // Reader uses ReaderArgs (flags before first transformer trigger or -o).
                // Writer uses WriterArgs (flags after -o).
                // Falls back to trimmed Arguments for legacy/YAML jobs that don't have stage args.
                var stageArgs = isWriter
                    ? (job.WriterArguments ?? FallbackTrimWriterArgs(job.Arguments))
                    : (job.ReaderArguments ?? FallbackTrimReaderArgs(job.Arguments));

                if (stageArgs != null && stageArgs.Length > 0)
                {
                    var tempRegistry = new Pipeline.FlagRegistry();
                    foreach (var f in contributor.GetFlagDefs()) tempRegistry.Register(f);
                    Pipeline.FlagBinder.Bind(instance, stageArgs, tempRegistry, factory.ComponentName);
                }

                // 3. YAML path: map non-null JobDefinition fields to adapter options
                MapProcessorProperties(job, instance, isWriter);

                _registry.RegisterByType(optionsType, instance);
            }
        }

        // Propagate global --key default to any writer that did not receive a per-branch key
        PropagateKey(globals?.Key);
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
                if (instance is IKeyAwareOptions keyAware && string.IsNullOrEmpty(keyAware.Key))
                {
                    keyAware.Key = key;
                    _registry.RegisterByType(optionsType, instance);
                }
            }
        }
    }

    private void MapProcessorProperties(JobDefinition job, object options, bool isWriter)
    {
        var type = options.GetType();

        // For readers: propagate SQL query (YAML path — CLI path handled by FlagBinder via QueryableReaderOptions.Query)
        if (!isWriter && !string.IsNullOrEmpty(job.Query))
            MapStringIfEmpty(type, options, "Query", job.Query);

        // For readers: RefAlias (SQL processors — from YAML or job routing)
        if (!isWriter && job.Ref != null && job.Ref.Length > 0)
        {
            var prop = type.GetProperty("RefAlias");
            if (prop != null && prop.PropertyType == typeof(string[]) && prop.CanWrite)
            {
                var current = prop.GetValue(options) as string[];
                if (current == null || current.Length == 0) prop.SetValue(options, job.Ref);
            }
        }

        // For writers: write options (YAML path — CLI path handled by FlagBinder via DbWriterOptions)
        if (isWriter)
        {
            MapString(type, options, "Table",      job.Table);
            MapString(type, options, "Strategy",   job.Strategy);
            MapString(type, options, "InsertMode", job.InsertMode);
            MapString(type, options, "Key",        job.Key);
            if (job.StrictSchema)           MapBool(type, options, "StrictSchema", true);
            if (job.AutoMigrate ?? false)   MapBool(type, options, "AutoMigrate", true);
            if (job.NoSchemaValidation)     MapBool(type, options, "NoSchemaValidation", true);
            MapString(type, options, "PreExec",     job.PreExec);
            MapString(type, options, "PostExec",    job.PostExec);
            MapString(type, options, "OnErrorExec", job.OnErrorExec);
            MapString(type, options, "FinallyExec", job.FinallyExec);
        }

        // For all: Arrow schema injection (--export-job / --job YAML)
        MapString(type, options, "Schema", job.Schema);
    }

    // Fallbacks for legacy/YAML jobs that don't have stage-scoped args.
    // Replicates the old -o trimming behaviour.
    private static string[]? FallbackTrimReaderArgs(string[]? args)
    {
        if (args == null || args.Length == 0) return args;
        int outIdx = Array.FindIndex(args, a =>
            string.Equals(a, "-o", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(a, "--output", StringComparison.OrdinalIgnoreCase));
        return outIdx > 0 ? args[..outIdx] : args;
    }

    private static string[]? FallbackTrimWriterArgs(string[]? args) => args;

    private static void MapString(Type type, object options, string propName, string? value)
    {
        if (string.IsNullOrEmpty(value)) return;
        var prop = type.GetProperty(propName);
        if (prop != null && prop.CanWrite && prop.PropertyType == typeof(string))
            prop.SetValue(options, value);
    }

    private static void MapStringIfEmpty(Type type, object options, string propName, string? value)
    {
        if (string.IsNullOrEmpty(value)) return;
        var prop = type.GetProperty(propName);
        if (prop == null || !prop.CanWrite || prop.PropertyType != typeof(string)) return;
        var current = prop.GetValue(options) as string;
        if (string.IsNullOrEmpty(current)) prop.SetValue(options, value);
    }

    private static void MapBool(Type type, object options, string propName, bool value)
    {
        var prop = type.GetProperty(propName);
        if (prop != null && prop.CanWrite && prop.PropertyType == typeof(bool))
            prop.SetValue(options, value);
    }
}
