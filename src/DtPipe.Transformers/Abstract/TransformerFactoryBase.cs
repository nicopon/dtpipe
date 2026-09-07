using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Transformers.Abstract;

/// <summary>
/// F12 — shared scaffolding for transformer factories: component metadata plumbing,
/// the typed <c>object → TOptions</> dispatch and the per-factory OptionsRegistry.
/// Subclasses implement only the typed creation surface.
/// </summary>
public abstract class TransformerFactoryBase<TOptions> : IDataTransformerFactory
    where TOptions : class, new()
{
    protected readonly OptionsRegistry Registry;

    protected TransformerFactoryBase(OptionsRegistry registry)
    {
        Registry = registry;
    }

    /// <summary>For factories that do not consume registry-bound defaults.</summary>
    protected TransformerFactoryBase()
    {
        Registry = null!;
    }

    public abstract string ComponentName { get; }

    public abstract string Category { get; }

    public virtual Type OptionsType => typeof(TOptions);

    public bool CanHandle(string connectionString) => false;

    public IDataTransformer? CreateFromOptions(object options)
        => options is TOptions typed ? CreateFromTypedOptions(typed) : null;

    /// <summary>Typed creation entry point implemented by each concrete factory.</summary>
    protected abstract IDataTransformer? CreateFromTypedOptions(TOptions options);

    public abstract IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration);

    public abstract object? CreateOptionsFromYaml(TransformerConfig config);
}
