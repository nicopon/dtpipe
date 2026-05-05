
using System.CommandLine;
using DtPipe.Core.Options;

namespace DtPipe.Cli.Infrastructure;

public interface ICliContributor
{
	/// <summary>
	/// Generates System.CommandLine Options for help display and TransformerPipelineBuilder.
	/// </summary>
	IEnumerable<Option> GetCliOptions();

	/// <summary>
	/// Generates FlagDef entries for the new PipelineLexer / FlagRegistry.
	/// </summary>
	IEnumerable<Pipeline.FlagDef> GetFlagDefs() => System.Linq.Enumerable.Empty<Pipeline.FlagDef>();

	// "Reader Options", "Writer Options", "Transformer Options"
	string Category { get; }
}
