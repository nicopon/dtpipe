using DtPipe.Core.Options;
using DtPipe.Configuration;
using DtPipe.Cli.Abstractions;

namespace DtPipe.Core.Abstractions;

public interface IDataTransformerFactory : ICliContributor
{
    /// <summary>
    /// Transformer type identifier for YAML matching and CLI prefix.
    /// Example: "fake" matches YAML type and --fake-* options.
    /// </summary>
    string TransformerType { get; }

    /// <summary>
    /// Creates a transformer instance from a global options context.
    /// This is legacy/unordered mode where options are pre-parsed into the registry.
    /// </summary>
    IDataTransformer? Create(DumpOptions options);

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
