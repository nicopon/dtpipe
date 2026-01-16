using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Configuration;
using QueryDump.Core.Options;

namespace QueryDump.Core;

public interface ITransformerFactory : ICliContributor
{
    /// <summary>
    /// Creates the transformer if configured, or null if disabled.
    /// </summary>
    IDataTransformer? Create(DumpOptions options);

    /// <summary>
    /// Returns the option types required by this transformer (e.g. typeof(NullOptions)).
    /// </summary>
    IEnumerable<Type> GetSupportedOptionTypes();
}
