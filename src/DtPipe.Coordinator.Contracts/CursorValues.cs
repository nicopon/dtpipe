using System.Text.RegularExpressions;

namespace DtPipe.Coordinator.Contracts;

/// <summary>
/// What a cursor's value may be. It is substituted as text into the query of a source brick, and a person may set it, so a value
/// is kept and handed on only if it is of the type it was kept with and cannot end the string it is written in: an integer, an
/// ISO 8601 date or instant, or a short text of letters, digits, spaces and <c>_ . : + / @ -</c>. The coordinator, the node and the
/// route that sets a value all apply this one rule.
/// </summary>
public static partial class CursorValues
{
    public const string Integer = "integer";
    public const string DateTime = "datetime";
    public const string Text = "string";

    /// <summary>The longest value kept.</summary>
    public const int MaxLength = 200;

    /// <summary>The type word as the state file spells it, lowercased, or <c>null</c> when it names none.</summary>
    public static string? TypeOf(string? type) => type?.Trim().ToLowerInvariant() switch
    {
        Integer => Integer,
        DateTime => DateTime,
        Text => Text,
        _ => null,
    };

    /// <summary>Why <paramref name="value"/> is not a value of <paramref name="type"/>, or <c>null</c> when it is.</summary>
    public static string? Refusal(string? type, string? value)
    {
        if (TypeOf(type) is not { } known) return $"the type '{type}' is none of integer, datetime, string.";
        if (string.IsNullOrEmpty(value)) return "the value is empty.";
        if (value.Length > MaxLength) return $"the value is longer than {MaxLength} characters.";
        var ok = known switch
        {
            Integer => IntegerPattern().IsMatch(value),
            DateTime => DatePattern().IsMatch(value) || InstantPattern().IsMatch(value),
            _ => TextPattern().IsMatch(value),
        };
        return ok ? null : known switch
        {
            Integer => "the value is not an integer.",
            DateTime => "the value is not an ISO 8601 date or instant.",
            _ => "the value holds a character other than letters, digits, spaces and _ . : + / @ -.",
        };
    }

    [GeneratedRegex(@"^-?[0-9]{1,30}\z")]
    private static partial Regex IntegerPattern();

    [GeneratedRegex(@"^[0-9]{4}-[0-9]{2}-[0-9]{2}\z")]
    private static partial Regex DatePattern();

    [GeneratedRegex(@"^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,7})?(Z|[+-][0-9]{2}:[0-9]{2})?\z")]
    private static partial Regex InstantPattern();

    [GeneratedRegex(@"^[A-Za-z0-9 _.:+/@-]+\z")]
    private static partial Regex TextPattern();
}
