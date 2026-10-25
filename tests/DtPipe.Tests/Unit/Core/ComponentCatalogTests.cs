using DtPipe.Cli.Infrastructure;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// The component catalog: discovery finds every documented provider, ordering is
/// deterministic (ComponentName sort), and duplicate names are detected.
/// </summary>
public class ComponentCatalogTests
{
    private static ComponentCatalog Discover()
        => ComponentCatalog.Discover(
            typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly,
            typeof(DtPipe.Processors.Sql.CompositeSqlTransformerFactory).Assembly,
            typeof(DtPipe.Transformers.Services.JsEngineProvider).Assembly);

    [Fact]
    public void Discovery_Finds_All_Providers_From_Reference()
    {
        var catalog = Discover();

        var expected = new[] { "duck", "sqlite", "pg", "ora", "mssql", "csv", "jsonl", "xml", "arrow", "parquet", "generate", "null", "checksum" };
        var readerNames = catalog.Readers.Select(e => e.ComponentName).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var writerNames = catalog.Writers.Select(e => e.ComponentName).ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var p in expected)
        {
            Assert.True(readerNames.Contains(p) || writerNames.Contains(p), $"provider '{p}' not discovered");
        }
    }

    [Fact]
    public void Discovery_Finds_Both_Stream_Processors_And_Ten_Transformers()
    {
        var catalog = Discover();

        Assert.Equal(2, catalog.StreamTransformers.Count);
        Assert.Equal(10, catalog.Transformers.Count);
    }

    [Fact]
    public void Ordering_Is_Deterministic_By_ComponentName()
    {
        var a = Discover().Readers.Select(e => e.ComponentName).ToList();
        var b = Discover().Readers.Select(e => e.ComponentName).ToList();

        Assert.Equal(a, b);
        Assert.Equal(a.OrderBy(x => x, StringComparer.OrdinalIgnoreCase), a);
    }

    [Fact]
    public void Excluded_Legacy_Descriptors_Are_Not_Discovered()
    {
        var catalog = Discover();
        Assert.DoesNotContain(catalog.Readers, e => e.ImplementationType.Name == "DuckDataSourceReaderDescriptor");
        Assert.DoesNotContain(catalog.StreamTransformers, e => e.ImplementationType.Name == "DuckDBSqlTransformerFactory");
    }

    [Fact]
    public void Validate_Throws_On_Duplicate_Names()
    {
        var entry = new ComponentCatalog.CatalogEntry("dup", typeof(object));
        var catalog = new ComponentCatalog(
            new List<ComponentCatalog.CatalogEntry> { entry, entry },
            new List<ComponentCatalog.CatalogEntry>(),
            new List<ComponentCatalog.CatalogEntry>(),
            new List<ComponentCatalog.CatalogEntry>());

        Assert.Throws<InvalidOperationException>(() => catalog.Validate());
    }
}
