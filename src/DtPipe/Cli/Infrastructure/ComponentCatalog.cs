using DtPipe.Core.Abstractions;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// The component catalog: a single deterministic discovery pass in which every
/// descriptor/factory in the supplied assemblies is found via its interface, instantiated
/// once, and sorted by ComponentName so help/completion ordering never depends on assembly
/// scan order.
/// </summary>
public sealed class ComponentCatalog
{
    public sealed record CatalogEntry(string ComponentName, Type ImplementationType);

    public IReadOnlyList<CatalogEntry> Readers { get; }
    public IReadOnlyList<CatalogEntry> Writers { get; }
    public IReadOnlyList<CatalogEntry> StreamTransformers { get; }
    public IReadOnlyList<CatalogEntry> Transformers { get; }

    internal ComponentCatalog(
        List<CatalogEntry> readers,
        List<CatalogEntry> writers,
        List<CatalogEntry> streamTransformers,
        List<CatalogEntry> transformers)
    {
        Readers = readers;
        Writers = writers;
        StreamTransformers = streamTransformers;
        Transformers = transformers;
    }

    /// <summary>
    /// Concrete types that exist in an assembly but are intentionally NOT registered
    /// (legacy/dormant descriptors). Mirrors the historical manual manifest.
    /// </summary>
    public static readonly IReadOnlySet<string> ExcludedTypes = new HashSet<string>(StringComparer.Ordinal)
    {
        "DtPipe.Adapters.DuckDB.DuckDataSourceReaderDescriptor",
        "DtPipe.Processors.DuckDB.DuckDBSqlTransformerFactory",
    };

    public static ComponentCatalog Discover(params System.Reflection.Assembly[] assemblies)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var readers = new List<CatalogEntry>();
        var writers = new List<CatalogEntry>();
        var stream = new List<CatalogEntry>();
        var transformers = new List<CatalogEntry>();

        foreach (var asm in assemblies)
        {
            foreach (var type in asm.GetTypes())
            {
                if (type.IsInterface || type.IsAbstract || type.ContainsGenericParameters) continue;
                if (!seen.Add(type.FullName!)) continue;
                if (ExcludedTypes.Contains(type.FullName!)) continue;

                // Host-side DI wrappers (CliProviderFactory & co.) require service
                // arguments and are registered explicitly instead — never discovered.
                if (type.Namespace?.StartsWith("DtPipe.Cli.Infrastructure") == true && type.Name.StartsWith("Cli")) continue;

                if (typeof(IProviderDescriptor<IStreamReader>).IsAssignableFrom(type) && !typeof(IProviderDescriptor<IDataWriter>).IsAssignableFrom(type))
                    readers.Add(new CatalogEntry(((IProviderDescriptor<IStreamReader>)NewInstance(type)).ComponentName, type));
                else if (typeof(IProviderDescriptor<IDataWriter>).IsAssignableFrom(type))
                    writers.Add(new CatalogEntry(((IProviderDescriptor<IDataWriter>)NewInstance(type)).ComponentName, type));
                else if (typeof(IStreamTransformerFactory).IsAssignableFrom(type))
                    stream.Add(new CatalogEntry(((IStreamTransformerFactory)NewInstance(type)).ComponentName, type));
                else if (typeof(IDataTransformerFactory).IsAssignableFrom(type))
                {
                    var componentName = TryGetComponentName(type);
                    if (componentName != null)
                        transformers.Add(new CatalogEntry(componentName, type));
                }
            }
        }

        return new ComponentCatalog(
            Order(readers), Order(writers), Order(stream), Order(transformers));

        static List<CatalogEntry> Order(List<CatalogEntry> list)
            => list.OrderBy(e => e.ComponentName, StringComparer.OrdinalIgnoreCase).ToList();
    }

    private static object NewInstance(Type type)
        => Activator.CreateInstance(type)
           ?? throw new InvalidOperationException($"Cannot instantiate component '{type.Name}' (parameterless constructor required).");

    /// <summary>
    /// Best-effort instantiation purely to READ ComponentName for transformer factories,
    /// whose constructors take DI dependencies that are not available during discovery.
    /// Real construction still happens through ActivatorUtilities at registration time.
    /// </summary>
    private static string? TryGetComponentName(Type type)
    {
        var registry = new DtPipe.Core.Options.OptionsRegistry();
        object?[]?[] shapes =
        [
            [],
            [registry],
            [registry, new DtPipe.Transformers.Services.JsEngineProvider()],
            [registry, new DtPipe.Transformers.Services.JsEngineProvider(), null],
            [new DtPipe.Transformers.Services.JsEngineProvider()],
        ];
        foreach (var shape in shapes)
        {
            try
            {
                if (Activator.CreateInstance(type, shape) is IDataTransformerFactory f)
                    return f.ComponentName;
            }
            catch { /* try next shape */ }
        }
        return null;
    }

    /// <summary>Duplicate component names across the same category are a wiring error.</summary>
    public void Validate()
    {
        void EnsureUnique(IReadOnlyList<CatalogEntry> list, string kind)
        {
            var dupes = list.GroupBy(e => e.ComponentName, StringComparer.OrdinalIgnoreCase)
                            .Where(g => g.Count() > 1).Select(g => g.Key).ToList();
            if (dupes.Count > 0)
                throw new InvalidOperationException($"Duplicate {kind} component names: {string.Join(", ", dupes)}");
        }
        EnsureUnique(Readers, "reader");
        EnsureUnique(Writers, "writer");
        EnsureUnique(StreamTransformers, "stream transformer");
        EnsureUnique(Transformers, "transformer");
    }
}
