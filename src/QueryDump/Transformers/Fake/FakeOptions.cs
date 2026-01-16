using System.ComponentModel;
using QueryDump.Core.Options;

namespace QueryDump.Transformers.Fake;

public record FakeOptions : ITransformerOptions
{
    public static string Prefix => "fake";
    
    [Description("Column:faker.method mapping for fake data (repeatable)")]
    public IReadOnlyList<string> Mappings { get; init; } = [];  // e.g., "MYCITY:address.city"
    
    [Description("Bogus locale for fake data (fr, de, es, etc.)")]
    public string Locale { get; init; } = "en";                  // Bogus locale (fr, de, etc.)
    
    [Description("Seed for reproducible fake data")]
    public int? Seed { get; init; } = null;                      // Optional seed for determinism
}
