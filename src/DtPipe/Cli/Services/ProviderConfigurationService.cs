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
/// - CLI path: FlagBinder reads adapter-specific flags directly from stage-scoped args
///   (ReaderArgs, WriterArgs). All adapter flags are declared via [ComponentOption].
/// - YAML path: ConfigurationBinder reads from ProviderOptions dictionaries.
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

                _registry.RegisterByType(optionsType, instance);
            }
        }

        // Propagate global --key default to any writer that did not receive a per-branch key
        string? globalKey = null;
        if (globals?.AllFlags.TryGetValue("--key", out var keyVal) == true)
            globalKey = keyVal?.ToString();
        else if (globals?.AllFlags.TryGetValue("-k", out var kVal) == true)
            globalKey = kVal?.ToString();
        PropagateKey(globalKey);
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
}

