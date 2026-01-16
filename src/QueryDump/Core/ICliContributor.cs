using System.CommandLine;
using System.CommandLine.Parsing;
using QueryDump.Core.Options;

namespace QueryDump.Core;

public interface ICliContributor
{
    IEnumerable<Option> GetCliOptions();
    void BindOptions(ParseResult parseResult, OptionsRegistry registry);
    
    // "Reader Options", "Writer Options", "Transformer Options"
    string Category { get; }
}
