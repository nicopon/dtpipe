using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Fake;

public record FakeOptions : ITransformerOptions
{
    public static string Prefix => "fake";
    public static string DisplayName => "Anonymization";
    
    [CliOption("--fake", Description = "Column:faker.method mapping (repeatable, e.g. 'NAME:name.firstname')")]
    public IReadOnlyList<string> Mappings { get; init; } = [];
    
    [Description("Bogus locale for fake data (fr, de, es, etc.)")]
    public string Locale { get; init; } = "en";
    
    [Description("Seed for reproducible fake data")]
    public int? Seed { get; init; } = null;
    
    [Description("Column to use as seed for deterministic fake data (value-based)")]
    public string? SeedColumn { get; init; } = null;
    
    [Description("Size of precomputed value table for deterministic mode (power of 2)")]
    public int TableSize { get; init; } = 65536;
    
    [Description("Enable deterministic mode using row index as seed (when no seed column)")]
    public bool Deterministic { get; init; } = false;
}
