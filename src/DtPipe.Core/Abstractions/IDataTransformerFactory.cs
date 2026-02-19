using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;

namespace DtPipe.Core.Abstractions;


public interface IDataTransformerFactory : IDataFactory
{
	/// <summary>
	/// Transformer type identifier for YAML matching and CLI prefix.
	/// Example: "fake" matches YAML type and --fake-* options.
	/// </summary>
	string TransformerType { get; }

	// Create(DumpOptions) removed as legacy

	/// <summary>
	/// Creates a transformer instance from specific configuration values.
	/// Example: config=[("--fake", "NAME:name.firstName"), ("--fake-locale", "fr")]
	/// This is used for the ordered pipeline where we group values by factory.
	/// </summary>
	/// <param name="configuration">The ordered list of (Option, Value) pairs provided in the CLI arguments.</param>
	IDataTransformer CreateFromConfiguration(IEnumerable<(string Option, string Value)> configuration);

	/// <summary>
	/// Creates a transformer instance from YAML TransformerConfig.
	/// </summary>
	/// <param name="config">The YAML transformer configuration with Mappings and Options dictionaries.</param>
	IDataTransformer? CreateFromYamlConfig(TransformerConfig config);
}
