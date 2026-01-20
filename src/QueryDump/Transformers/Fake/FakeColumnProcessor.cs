using Bogus;

namespace QueryDump.Transformers.Fake;

/// <summary>
/// Holds pre-computed processing info for a column in FakeDataTransformer.
/// Can be either a generator-based processor or a template-based processor.
/// </summary>
public readonly struct FakeColumnProcessor
{
    public readonly int Index;
    public readonly Func<Faker, object?>? Generator;
    public readonly string? Template;
    public readonly HashSet<string>? ReferencedColumns;
    public readonly string? FakerPath; // For deterministic mode hashing
    public readonly uint FakerHash; // Precomputed hash

    public bool IsTemplate => Template is not null;

    /// <summary>
    /// Constructor for faker/hardcoded generator.
    /// </summary>
    public FakeColumnProcessor(int index, Func<Faker, object?>? generator, string? fakerPath = null)
    {
        Index = index;
        Generator = generator;
        Template = null;
        ReferencedColumns = null;
        FakerPath = fakerPath;
        FakerHash = fakerPath is not null ? StableHash.ComputeFromString(fakerPath) : 0;
    }

    /// <summary>
    /// Constructor for template processor.
    /// </summary>
    public FakeColumnProcessor(int index, string template, HashSet<string> referencedColumns)
    {
        Index = index;
        Generator = null;
        Template = template;
        ReferencedColumns = referencedColumns;
        FakerPath = null;
        FakerHash = 0;
    }
}
