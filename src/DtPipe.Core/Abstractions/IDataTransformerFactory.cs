using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Core.Abstractions;


public interface IDataTransformerFactory : IDataFactory
{

	// Create(DumpOptions) removed as legacy

	/// <summary>
	/// Creates a transformer instance from specific configuration values.
	/// Example: config=[("--fake", "NAME:name.firstName"), ("--fake-locale", "fr")]
	/// This is used for the ordered pipeline where we group values by factory.
	/// </summary>
	/// <param name="configuration">The ordered list of (Option, Value) pairs provided in the CLI arguments.</param>
	IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration);

	/// <summary>
	/// Turns the <c>mappings:</c> half of a YAML transformer block into this transformer's own
	/// options object, or null when there is nothing to build. How a mapping encodes — keys only,
	/// "key:value" pairs, values joined into a script — is per-transformer and stays here.
	///
	/// <para>
	/// The <c>options:</c> half is NOT read here. It is bound reflectively onto the returned object
	/// by the caller, against the same properties the help prints. Reading it here by hand is how a
	/// key advertised by the help came to bind nothing and say nothing.
	/// </para>
	/// </summary>
	/// <param name="config">The YAML transformer configuration with Mappings and Options dictionaries.</param>
	object? CreateOptionsFromYaml(TransformerConfig config);

	/// <summary>
	/// Creates a transformer from a pre-bound options object (typically populated by OptionBinder).
	/// The options type must match the factory's OptionsType.
	/// </summary>
	IDataTransformer? CreateFromOptions(object options);
}
