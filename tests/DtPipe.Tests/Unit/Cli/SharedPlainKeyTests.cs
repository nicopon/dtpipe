using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Services;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A plain provider-options key ("csv:") feeds a reader and a writer that share a component name,
/// so a key meant for one side reaches the other and matches nothing there. Silencing that was one
/// switch, and it also swallowed a key NEITHER side declares: a misspelling under "pg:" was lost
/// without a word, exit 0, and --strict-bindings — whose whole job is to refuse exactly that —
/// never got a verdict.
///
/// The two halves are tested together on purpose. A fix that makes unknown keys loud is worthless
/// if it also makes every legitimate shared block noisy, and that is the easy way to get it wrong.
/// </summary>
public class SharedPlainKeyTests
{
	private static (OptionsRegistry Registry, ProviderConfigurationService Service) Build()
	{
		var services = new ServiceCollection();
		services.AddLogging();
		var registry = new OptionsRegistry();
		services.AddSingleton(registry);
		var sp = services.BuildServiceProvider();

		var contributors = new ICliContributor[]
		{
			new CliStreamReaderFactory(new DtPipe.Adapters.Csv.CsvReaderDescriptor(), registry, sp),
			new CliDataWriterFactory(new DtPipe.Adapters.Csv.CsvWriterDescriptor(), registry, sp)
		};

		return (registry, new ProviderConfigurationService(contributors, registry));
	}

	private static JobDefinition JobWith(params (string Key, object? Value)[] options)
		=> new()
		{
			Input = "csv:in.csv",
			Output = "csv:out.csv",
			ProviderOptions = new Dictionary<string, Dictionary<string, object?>>
			{
				["csv"] = options.ToDictionary(o => o.Key, o => o.Value)
			}
		};

	/// <summary>A key that belongs to the other role is this role's business to ignore, quietly.</summary>
	[Theory]
	[InlineData("has-header")]   // reader only
	[InlineData("null-value")]   // writer only
	public void A_Key_The_Other_Role_Owns_Is_Accepted_In_Silence(string key)
	{
		var (_, service) = Build();

		service.BindOptions(
			JobWith((key, "true")),
			globals: new GlobalOptions { StrictBindings = true });
	}

	/// <summary>
	/// The case the single switch swallowed. Strict mode must refuse it, because a plain key is
	/// what a hand-written job file uses and a tube shows none of it.
	/// </summary>
	[Fact]
	public void A_Key_Neither_Role_Declares_Is_Refused_In_Strict_Mode()
	{
		var (_, service) = Build();

		var ex = Assert.Throws<InvalidOperationException>(() =>
			service.BindOptions(
				JobWith(("delimiterr", ";")),
				globals: new GlobalOptions { StrictBindings = true }));

		Assert.Contains("delimiterr", ex.Message);
	}

	/// <summary>Dropping the other role's keys must not drop the ones this role does declare.</summary>
	[Fact]
	public void A_Key_This_Role_Declares_Still_Binds()
	{
		var (registry, service) = Build();

		service.BindOptions(JobWith(("separator", ";")));

		Assert.Equal(";", registry.Get<DtPipe.Adapters.Csv.CsvWriterOptions>().Separator);
	}
}
