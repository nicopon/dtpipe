using System;
using System.Linq;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F13 capability-completeness convention: every READER descriptor that declares
/// RequiresQuery must expose options implementing BOTH IQueryAwareOptions (the
/// fail-closed check in CliProviderFactory) and ITableAwareOptions (the --table
/// auto-build in LinearPipelineService).
///
/// Regression context: commit 85186aa8 introduced ITableAwareOptions but added it
/// only to writer option classes — QueryableReaderOptions was left out, so every
/// reader-side --table run failed with "A query is required". A convention scan
/// over the component catalog catches that class of error at the interface level,
/// not per-incident.
/// </summary>
public class ComponentCatalogCapabilityTests
{
    private static ComponentCatalog DiscoverCatalog()
        => ComponentCatalog.Discover(
            typeof(DtPipe.Program).Assembly,
            typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly,
            typeof(DtPipe.Processors.Sql.CompositeSqlTransformerFactory).Assembly,
            typeof(DtPipe.Transformers.Services.JsEngineProvider).Assembly);

    [Fact]
    public void Every_RequiresQuery_Reader_Options_Implement_Query_And_Table_Contracts()
    {
        var catalog = DiscoverCatalog();
        Assert.NotEmpty(catalog.Readers);

        var violations = catalog.Readers
            .Select(entry => (Entry: entry, Descriptor: (IProviderDescriptor<IStreamReader>)Activator.CreateInstance(entry.ImplementationType)!))
            .Where(pair => pair.Descriptor.RequiresQuery)
            .Where(pair => !typeof(IQueryAwareOptions).IsAssignableFrom(pair.Descriptor.OptionsType)
                           || !typeof(ITableAwareOptions).IsAssignableFrom(pair.Descriptor.OptionsType))
            .Select(pair => $"{pair.Entry.ComponentName} ({pair.Descriptor.OptionsType.Name})")
            .ToList();

        Assert.True(violations.Count == 0,
            "Readers declaring RequiresQuery must have options implementing IQueryAwareOptions " +
            "and ITableAwareOptions (--table auto-build). Violations: " + string.Join(", ", violations));
    }

    [Fact]
    public void Reader_And_Writer_Option_Types_Are_Instantiable()
    {
        // The bulk pass materializes defaults through Activator for every contributor;
        // an options type without a parameterless ctor fails only at runtime today.
        var catalog = DiscoverCatalog();

        foreach (var entry in catalog.Readers.Concat(catalog.Writers))
        {
            var descriptor = (IDataFactory)Activator.CreateInstance(entry.ImplementationType)!;
            var instance = Activator.CreateInstance(descriptor.OptionsType);
            Assert.NotNull(instance);
        }
    }
}
