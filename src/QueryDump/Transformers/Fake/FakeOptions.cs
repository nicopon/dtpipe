using System.ComponentModel;
using QueryDump.Core.Options;
using QueryDump.Core.Attributes;

namespace QueryDump.Transformers.Fake;

public record FakeOptions : ITransformerOptions
{
    public static string Prefix => "fake";
    public static string DisplayName => "Anonymization";
    
    [CliOption("--fake", Description = "Column:faker mapping (e.g. EMAIL:internet.email, NAME:name.fullName, EMAIL_ALT:internet.email#alt)")]
    public IReadOnlyList<string> Mappings { get; init; } = [];
    
    [Description("Locale for fake data (en, fr, de, es, ja, zh_CN...)")]
    public string Locale { get; init; } = "en";
    
    [Description("Global seed for reproducible random fakes across all columns")]
    public int? Seed { get; init; } = null;
    
    [Description("Column to use as seed (same value = same fake output, ideal for ID columns)")]
    public string? SeedColumn { get; init; } = null;
    
    [Description("Row-index based deterministic mode (row N always gets same values)")]
    public bool Deterministic { get; init; } = false;
    
    [Description("Skip fake generation when source value is null")]
    public bool SkipNull { get; init; } = false;
}
