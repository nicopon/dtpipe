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
/// - CLI path: OptionBinder.BindCli reads adapter-specific flags directly from stage-scoped args
///   (ReaderArgs, WriterArgs). All adapter flags are declared via [ComponentOption].
/// - YAML path: OptionBinder.BindYaml reads from ProviderOptions dictionaries.
/// </summary>
public class ProviderConfigurationService
{
    private readonly IEnumerable<ICliContributor> _contributors;
    private readonly OptionsRegistry _registry;
    private readonly Pipeline.FlagRegistry? _lineFlags;

    /// <param name="lineFlags">
    /// Every flag the command line may carry. A stage slice holds more than the component's own
    /// options — the boundary token that opened it, the engine controls — and
    /// <see cref="Pipeline.OptionBinder.BindCli"/> needs this to tell a flag that belongs
    /// elsewhere from one nobody declares.
    /// </param>
    public ProviderConfigurationService(IEnumerable<ICliContributor> contributors, OptionsRegistry registry,
        Pipeline.FlagRegistry? lineFlags = null)
    {
        _contributors = contributors;
        _registry = registry;
        _lineFlags = lineFlags;
    }

    public void BindOptions(JobDefinition job, Pipeline.CliJobContext? context = null, GlobalOptions? globals = null)
    {
        foreach (var contributor in _contributors)
        {
            if (contributor is IDataFactory factory)
            {
                var optionsType = factory.OptionsType;
                // Bulk pass: every contributor gets an instance, active or not. Most providers are
                // inactive in any given run, and registering them all is what lets a later Get
                // mean "no configuration pass reached this flow" rather than "this one was idle".
                var instance = _registry.Get(optionsType);
                bool isWriter = factory is IDataWriterFactory;

                // 1. Bind from ProviderOptions (YAML path) — only for the contributor that
                //    handles the active connection. Binding every homonymous contributor
                //    sprayed reader-only keys onto writers (and vice versa) and warned.
                if (job.ProviderOptions != null && HandlesActiveConnection(factory, isWriter ? job.Output : job.Input))
                {
                    bool strict = globals?.StrictBindings == true;

                    // Shared plain key ("csv:" feeding both sides of a same-named pair): some keys
                    // legitimately target only one side. Those are dropped here, before the binder
                    // sees them, rather than silenced inside it — silencing was a single switch
                    // that also swallowed a key NEITHER side declares, so a misspelling under
                    // "pg:" was lost without a word and --strict-bindings, whose whole job is to
                    // refuse exactly that, never got a verdict.
                    var sibling = _contributors.OfType<IDataFactory>().FirstOrDefault(s =>
                        !ReferenceEquals(s, factory)
                        && (s is IDataWriterFactory) != isWriter
                        && s.ComponentName.Equals(factory.ComponentName, StringComparison.OrdinalIgnoreCase));

                    if (job.ProviderOptions.TryGetValue(factory.ComponentName, out var opts))
                        Pipeline.OptionBinder.BindYaml(instance, KeysNotOwnedBy(sibling, optionsType, opts), strict);

                    var suffix = isWriter ? "-writer" : "-reader";
                    if (job.ProviderOptions.TryGetValue(factory.ComponentName + suffix, out var specificOpts))
                        Pipeline.OptionBinder.BindYaml(instance, specificOpts, strict);
                }

                // 2. Bind from stage-scoped args (CLI path).
                // Reader uses ReaderArgs; writer uses WriterArgs. Null for YAML jobs.
                // Only bind args for the contributor that actually handles the active connection,
                // to avoid spurious warnings when a flag value is valid for one provider's enum
                // but not another's (e.g. OracleInsertMode.Append vs PostgreSqlInsertMode).
                var stageArgs = isWriter ? context?.WriterArguments : context?.ReaderArguments;
                var activeConnection = isWriter ? job.Output : job.Input;

                if (stageArgs != null && stageArgs.Length > 0
                    && (string.IsNullOrEmpty(activeConnection) || factory.CanHandle(activeConnection)))
                {
                    var tempRegistry = new Pipeline.FlagRegistry();
                    foreach (var f in contributor.GetFlagDefs()) tempRegistry.Register(f);
                    Pipeline.OptionBinder.BindCli(instance, stageArgs, tempRegistry, factory.ComponentName,
                        strict: globals?.StrictBindings == true, lineFlags: _lineFlags);
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

    /// <summary>
    /// The keys of a shared plain block minus those that belong to the other role alone: what
    /// this options type accepts, plus what nobody declares — the second being the whole point,
    /// since an unknown key must still reach the binder's unknown-key path.
    /// </summary>
    private static IReadOnlyDictionary<string, object?> KeysNotOwnedBy(
        IDataFactory? sibling, Type optionsType, Dictionary<string, object?> opts)
    {
        if (sibling is null) return opts;

        return opts
            .Where(kv => Pipeline.OptionBinder.Accepts(optionsType, kv.Key)
                         || !Pipeline.OptionBinder.Accepts(sibling.OptionsType, kv.Key))
            .ToDictionary(kv => kv.Key, kv => kv.Value);
    }

    /// <summary>
    /// Same resolution rule as the engine's ResolveFactory: component-name prefix,
    /// bare component name, or the factory's own CanHandle verdict.
    /// </summary>
    private static bool HandlesActiveConnection(IDataFactory factory, string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return false;
        var raw = connectionString.Trim();
        return ComponentSelector.Matches(raw, factory.ComponentName)
            || factory.CanHandle(raw);
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

}

