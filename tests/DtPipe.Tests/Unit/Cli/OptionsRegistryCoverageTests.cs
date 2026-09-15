using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// After a configuration pass, every component of the catalogue has its options type in the
/// registry — so a later miss means one thing only: no pass reached that flow.
///
/// This is the half of the registry's old warning that was worth keeping. The warning itself was
/// user-facing and told whoever passed nothing that their values had been skipped; the condition
/// underneath it is a wiring invariant, decidable, and belongs here. A code path that reaches
/// <see cref="OptionsRegistry.Get{T}"/> on an unregistered type is a wiring bug, and this suite is
/// where it should turn red — not a line of stderr for someone who can do nothing about it.
/// </summary>
public class OptionsRegistryCoverageTests
{
	/// <summary>
	/// Derived from the catalogue, never listed, so a new adapter is covered by existing.
	/// </summary>
	private static ComponentCatalog DiscoverCatalog()
		=> ComponentCatalog.Discover(
			typeof(DtPipe.Program).Assembly,
			typeof(DtPipe.Adapters.Csv.CsvReaderDescriptor).Assembly,
			typeof(DtPipe.Processors.Sql.CompositeSqlTransformerFactory).Assembly,
			typeof(DtPipe.Transformers.Services.JsEngineProvider).Assembly);

	[Fact]
	public void A_Configuration_Pass_Leaves_No_Component_Unregistered()
	{
		var catalog = DiscoverCatalog();
		Assert.NotEmpty(catalog.Readers);
		Assert.NotEmpty(catalog.Writers);

		var services = new ServiceCollection();
		services.AddLogging();
		var registry = new OptionsRegistry();
		services.AddSingleton(registry);
		var sp = services.BuildServiceProvider();

		var contributors = new List<ICliContributor>();
		foreach (var entry in catalog.Readers)
			contributors.Add(new CliStreamReaderFactory(
				(IProviderDescriptor<IStreamReader>)Activator.CreateInstance(entry.ImplementationType)!, registry, sp));
		foreach (var entry in catalog.Writers)
			contributors.Add(new CliDataWriterFactory(
				(IProviderDescriptor<IDataWriter>)Activator.CreateInstance(entry.ImplementationType)!, registry, sp));

		new DtPipe.Cli.Services.ProviderConfigurationService(contributors, registry)
			.BindOptions(new JobDefinition { Input = "csv:in.csv", Output = "csv:out.csv" });

		var unregistered = contributors
			.OfType<IDataFactory>()
			.Where(f => !registry.TryGetByType(f.OptionsType, out _))
			.Select(f => $"{f.ComponentName} ({f.OptionsType.Name})")
			.Distinct()
			.ToList();

		Assert.True(unregistered.Count == 0,
			"A configuration pass must register every component's options type, active or not — "
			+ "that is what lets a miss mean 'no pass reached this flow'. Unregistered: "
			+ string.Join(", ", unregistered));
	}
}
