using QueryDump.Core.Options;
using QueryDump.Configuration;

namespace QueryDump.Core;

public interface IDataTransformerFactory : ICliContributor
{
    /// <summary>
    /// Creates a transformer instance from a global options context.
    /// This is legacy/unordered mode where options are pre-parsed into the registry.
    /// </summary>
    IDataTransformer? Create(DumpOptions options);

    /// <summary>
    /// Creates a transformer instance from specific configuration values.
    /// Example: values=["NAME:name.firstName", "EMAIL:internet.email"] for a FakeDataTransformerFactory.
    /// This is used for the ordered pipeline where we group values by factory.
    /// </summary>
    /// <param name="values">The raw string values provided for this transformer type in the CLI arguments.</param>
    IDataTransformer CreateFromConfiguration(IEnumerable<string> values);
}
