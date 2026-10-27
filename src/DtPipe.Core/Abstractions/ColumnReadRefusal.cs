namespace DtPipe.Core.Abstractions;

/// <summary>
/// One column a reader cannot read: its name, its type as the source spells it, and the expression,
/// in the source's own language, that selects it as a type the reader reads. A reader reads natively
/// every type it knows and refuses the rest; it never decodes one it does not know as text.
/// </summary>
public sealed record ColumnReadRefusal(string Column, string SourceType, string Remedy);

/// <summary>
/// Every column a reader refused, raised when it opens, before the first row. The reader names the
/// expressions; the text that relays them is composed here, the same for every reader.
/// <see cref="Exception.Message"/> is plain text the host formats.
/// </summary>
public sealed class ColumnReadRefusedException(string reader, IReadOnlyList<ColumnReadRefusal> refusals)
    : Exception(Compose(reader, refusals))
{
    public string Reader { get; } = reader;
    public IReadOnlyList<ColumnReadRefusal> Refusals { get; } = refusals;

    private static string Compose(string reader, IReadOnlyList<ColumnReadRefusal> refusals)
    {
        var lines = new List<string>
        {
            $"The {reader} reader cannot read {(refusals.Count == 1 ? "this column" : $"these {refusals.Count} columns")}, and nothing was read:"
        };
        foreach (var r in refusals)
            lines.Add($"  '{r.Column}': {r.SourceType}; select it as: {r.Remedy}");
        lines.Add("Write each expression in the query where the column is selected.");
        return string.Join(Environment.NewLine, lines);
    }
}
