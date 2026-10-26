using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions;
using DtPipe.Processors.DuckDB;

namespace DtPipe.Engine;

/// <summary>
/// The plain text a writer's refusal becomes: every refused column with what the writer accepts in
/// its place, and the SQL step that brings each one there. The engine relays what the writer said;
/// it holds no list of any writer's types and converts nothing.
/// </summary>
internal static class ColumnTypeAdvice
{
    public static ColumnTypeRefusedException Refuse(string writer, IReadOnlyList<ColumnTypeRefusal> refusals)
        => new(writer, refusals, Compose(writer, refusals));

    public static string Compose(string writer, IReadOnlyList<ColumnTypeRefusal> refusals)
    {
        var lines = new List<string>
        {
            $"The {writer} writer cannot write {(refusals.Count == 1 ? "this column" : $"these {refusals.Count} columns")} as {(refusals.Count == 1 ? "it arrives" : "they arrive")}, and nothing was written:"
        };

        var casts = new List<string>();
        foreach (var r in refusals)
        {
            var accepted = r.Accepted.Select(Describe).ToList();
            lines.Add(accepted.Count == 0
                ? $"  '{r.Received.Name}': {Describe(r.Received)}; no type it accepts keeps these values."
                : $"  '{r.Received.Name}': {Describe(r.Received)}; accepted instead: {string.Join(", ", accepted)}.");

            var column = Quote(r.Received.Name);
            var cast = r.Accepted.Select(a => DuckDbSqlTypeNames.CastTo(column, r.Received.DataType, a)).FirstOrDefault(c => c is not null);
            if (cast is not null)
                casts.Add($"{cast} AS {column}");
        }

        if (casts.Count > 0)
        {
            lines.Add("Cast each column to an accepted type in a SQL step before the writer: give this branch --alias src, " +
                      "and write to the target from the step:");
            lines.Add($"  --from src --sql \"SELECT * REPLACE ({string.Join(", ", casts)}) FROM src\" -o <target>");
        }

        return string.Join(Environment.NewLine, lines);
    }

    /// <summary>An Arrow type as a person reads it: its name, its unit or zone, its extension.</summary>
    internal static string Describe(Field field)
    {
        if (field.HasMetadata && field.Metadata.TryGetValue("ARROW:extension:name", out var ext))
        {
            if (ext == "arrow.opaque" && field.Metadata.TryGetValue("ARROW:extension:metadata", out var meta))
                return $"{ext} {meta}";
            return ext;
        }
        return Describe(field.DataType);
    }

    internal static string Describe(IArrowType type) => type switch
    {
        Decimal128Type d => $"decimal({d.Precision}, {d.Scale})",
        Decimal256Type d => $"decimal({d.Precision}, {d.Scale})",
        TimestampType ts => string.IsNullOrEmpty(ts.Timezone) ? $"timestamp[{Unit(ts.Unit)}]" : $"timestamp[{Unit(ts.Unit)}, {ts.Timezone}]",
        Time32Type t => $"time32[{Unit(t.Unit)}]",
        Time64Type t => $"time64[{Unit(t.Unit)}]",
        FixedSizeBinaryType f => $"fixed_size_binary[{f.ByteWidth}]",
        _ => type.Name
    };

    private static string Unit(TimeUnit u) => u switch
    {
        TimeUnit.Second => "s",
        TimeUnit.Millisecond => "ms",
        TimeUnit.Microsecond => "us",
        _ => "ns"
    };

    private static string Quote(string name)
        => name.All(c => char.IsAsciiLetterOrDigit(c) || c == '_') && !char.IsAsciiDigit(name[0])
            ? name
            : "\\\"" + name.Replace("\"", "\"\"") + "\\\"";
}
