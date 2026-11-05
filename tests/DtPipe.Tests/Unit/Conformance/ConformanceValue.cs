using System.Collections;
using System.Globalization;

namespace DtPipe.Tests.Unit.Conformance;

/// <summary>
/// The spelling a conformance cell compares: the value's CLR type and its invariant text. Comparing
/// text keeps a decimal's scale, which <see cref="decimal.Equals(decimal)"/> ignores. A
/// <see cref="DateTime"/> spells its kind unless it is zone-less, so a value that lets the host's zone
/// into the data path (<see cref="DateTimeKind.Local"/>) never equals an expected one, even on a host
/// in UTC.
/// </summary>
internal static class ConformanceValue
{
    public static string Of(object? value) => value switch
    {
        null => "null",
        string s => $"String:{s}",
        bool b => b ? "Boolean:true" : "Boolean:false",
        decimal d => $"Decimal:{d.ToString(CultureInfo.InvariantCulture)}",
        double d => $"Double:{d.ToString("R", CultureInfo.InvariantCulture)}",
        float f => $"Single:{f.ToString("R", CultureInfo.InvariantCulture)}",
        DateTime { Kind: DateTimeKind.Unspecified } d => $"DateTime:{d:O}",
        DateTime d => $"DateTime[{d.Kind}]:{d:O}",
        DateTimeOffset o => $"DateTimeOffset:{o:O}",
        DateOnly d => $"DateOnly:{d:O}",
        TimeOnly t => $"TimeOnly:{t:O}",
        TimeSpan t => $"TimeSpan:{t:c}",
        Guid g => $"Guid:{g}",
        byte[] b => $"Bytes:{Convert.ToHexString(b)}",
        IDictionary d => "{" + string.Join(",", Entries(d)) + "}",
        IEnumerable e => "[" + string.Join(",", e.Cast<object?>().Select(Of)) + "]",
        IFormattable f => $"{value.GetType().Name}:{f.ToString(null, CultureInfo.InvariantCulture)}",
        _ => $"{value.GetType().Name}:{value}"
    };

    private static IEnumerable<string> Entries(IDictionary dictionary)
    {
        var entries = new List<string>();
        foreach (DictionaryEntry entry in dictionary) entries.Add($"{entry.Key}={Of(entry.Value)}");
        entries.Sort(StringComparer.Ordinal);
        return entries;
    }
}
