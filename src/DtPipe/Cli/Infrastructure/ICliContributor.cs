
using DtPipe.Core.Options;

namespace DtPipe.Cli.Infrastructure;

public interface ICliContributor
{
	/// <summary>
	/// Generates FlagDef entries for the PipelineLexer / FlagRegistry / HelpRenderer.
	/// Single source of truth for CLI flag definitions.
	/// </summary>
	IEnumerable<Pipeline.FlagDef> GetFlagDefs() => System.Linq.Enumerable.Empty<Pipeline.FlagDef>();

	// "Reader Options", "Writer Options", "Transformer Options"
	string Category { get; }
}
