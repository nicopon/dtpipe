
using System.CommandLine;
using System.CommandLine.Parsing;
using DtPipe.Core.Options;
using DtPipe.Cli;
using System.Collections.Generic;

namespace DtPipe.Cli.Infrastructure;

public interface ICliContributor
{
	/// <summary>
	/// Generates the System.CommandLine Options associated with this contributor.
	/// </summary>
	IEnumerable<Option> GetCliOptions();

	/// <summary>
	/// Returns the pipeline phase associated with each flag name exposed by this contributor.
	/// Used by the autocompletion engine to filter irrelevant suggestions.
	/// Key: flag name (long form, e.g. "--csv-separator"). Value: CliPipelinePhase.
	/// </summary>
	IReadOnlyDictionary<string, CliPipelinePhase> FlagPhases => new Dictionary<string, CliPipelinePhase>();

	/// <summary>
	/// Dependencies required for a flag to be visible in autocompletion.
	/// Key: dependent flag (e.g. "--fake-locale").
	/// Value: required flag (e.g. "--fake").
	/// </summary>
	IReadOnlyDictionary<string, string> FlagDependencies => new Dictionary<string, string>();

	/// <summary>
	/// The ComponentName of the provider this contributor is bound to (e.g. "csv", "fake").
	/// Used to match Reader options against the current --input prefix.
	/// Null for contributors that are not tied to a specific provider (e.g. global transformers).
	/// </summary>
	string? BoundComponentName => null;
	void BindOptions(ParseResult parseResult, OptionsRegistry registry);

	// "Reader Options", "Writer Options", "Transformer Options"
	string Category { get; }

	/// <summary>
	/// Allows the contributor to handle the command execution itself (e.g. for listing options, version info).
	/// Returns an exit code if handled, or null to continue normal execution flow.
	/// </summary>
	Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default) => Task.FromResult<int?>(null);
}
