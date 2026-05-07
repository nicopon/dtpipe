using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace DtPipe.Cli.Pipeline;

/// <summary>
/// Centralised construction of the <see cref="FlagRegistry"/> used by both
/// <c>PipelineLexer</c> (parsing) and <c>HandleSuggest</c> (autocompletion).
/// Single source of truth: core structural flags + component-contributed flags.
/// </summary>
public static class FlagRegistryFactory
{
    public static FlagRegistry Build(IServiceProvider sp)
    {
        var registry = new FlagRegistry();

        // 1. Structural flags — required by PipelineLexer for branch-splitting / routing
        CoreFlagRegistry.RegisterCoreFlags(registry);

        // 2. Universal engine controls (--batch-size, --limit, --sampling-rate, --sampling-seed, --prefix)
        RegisterWithStage(registry, new PipelineOptionsCliContributor(), FlagStage.All);

        // 3. Component-contributed flags — readers, writers, transformers
        var readers      = sp.GetRequiredService<IEnumerable<IStreamReaderFactory>>().OfType<ICliContributor>();
        var writers      = sp.GetRequiredService<IEnumerable<IDataWriterFactory>>().OfType<ICliContributor>();
        var transformers = sp.GetRequiredService<IEnumerable<IDataTransformerFactory>>().OfType<ICliContributor>();

        foreach (var c in readers)      RegisterWithStage(registry, c, FlagStage.Reader, ((IDataFactory)c).ComponentName);
        foreach (var c in writers)      RegisterWithStage(registry, c, FlagStage.Writer, ((IDataFactory)c).ComponentName);
        foreach (var c in transformers) RegisterWithStage(registry, c, FlagStage.Pipeline, ((IDataTransformerFactory)c).ComponentName);

        // 4. Stream processor trigger flags (--sql, --merge, etc.)
        foreach (var stf in sp.GetRequiredService<IEnumerable<IStreamTransformerFactory>>())
        {
            foreach (var (flag, isBoolean) in stf.CliTriggerFlags)
            {
                registry.Register(new FlagDef(
                    flag,
                    Array.Empty<string>(),
                    isBoolean ? FlagArity.Boolean : FlagArity.Scalar,
                    FlagScope.PerBranch,
                    stf.ComponentName,
                    FlagStage.Pipeline));
            }
        }

        registry.Validate();
        return registry;
    }

    private static void RegisterWithStage(FlagRegistry registry, ICliContributor contributor, FlagStage stage, string? componentName = null)
    {
        foreach (var def in contributor.GetFlagDefs())
            registry.Register(def with { Stage = stage, ComponentName = componentName ?? def.ComponentName });
    }
}
