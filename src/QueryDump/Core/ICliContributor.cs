using System.CommandLine;
using QueryDump.Core.Options;

namespace QueryDump.Core;

public interface ICliContributor
{
    IEnumerable<Option> GetCliOptions();
    void BindOptions(ParseResult parseResult, OptionsRegistry registry);
    
    // "Reader Options", "Writer Options", "Transformer Options"
    // "Reader Options", "Writer Options", "Transformer Options"
    string Category { get; }

    /// <summary>
    /// Allows the contributor to handle the command execution itself (e.g. for listing options, version info).
    /// Returns an exit code if handled, or null to continue normal execution flow.
    /// </summary>
    Task<int?> HandleCommandAsync(ParseResult parseResult, CancellationToken ct = default) => Task.FromResult<int?>(null);
}
