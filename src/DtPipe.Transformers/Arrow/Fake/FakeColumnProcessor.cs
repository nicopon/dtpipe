namespace DtPipe.Transformers.Arrow.Fake;

internal readonly struct FakeTemplateSegment
{
    public readonly bool IsLiteral;
    public readonly string Literal;
    public readonly int ColumnIndex;

    private FakeTemplateSegment(bool isLiteral, string literal, int columnIndex)
    {
        IsLiteral = isLiteral;
        Literal = literal;
        ColumnIndex = columnIndex;
    }

    public static FakeTemplateSegment CreateLiteral(string literal) => new(true, literal, -1);
    public static FakeTemplateSegment CreateColumn(int columnIndex) => new(false, string.Empty, columnIndex);
}

/// <summary>
/// Holds pre-computed processing info for a column in <see cref="FakeDataTransformer"/>.
/// </summary>
internal readonly struct FakeColumnProcessor
{
    public readonly int Index;
    public readonly IFakeColumnGenerator? Generator;
    public readonly FakeTemplateSegment[]? TemplateSegments;
    public readonly int[]? ReferencedIndices;
    public readonly ulong ColSalt;

    public bool IsTemplate => TemplateSegments is not null;
    public bool IsActive => Generator is not null || TemplateSegments is not null;

    public FakeColumnProcessor(int index, IFakeColumnGenerator generator, ulong colSalt)
    {
        Index = index;
        Generator = generator;
        TemplateSegments = null;
        ReferencedIndices = null;
        ColSalt = colSalt;
    }

    public FakeColumnProcessor(int index, FakeTemplateSegment[] templateSegments, int[] referencedIndices)
    {
        Index = index;
        Generator = null;
        TemplateSegments = templateSegments;
        ReferencedIndices = referencedIndices;
        ColSalt = 0;
    }
}
