using System.Globalization;
using System.Text;

namespace DtPipe.Core.Helpers;

/// <summary>
/// How a text reads as a fractional number, wherever a reader or a writer's converter reads one, under
/// the invariant culture. No group separator: <c>1,5</c> would read as 15 and <c>1,234</c> as 1234, which
/// is what the CSV writer's <c>--csv-decimal-separator ,</c> leaves for 1.5 and 1.234, and the place of
/// the separator is not checked. A number past the type's range is refused, never read as ±∞; the
/// spelled-out <c>NaN</c> and <c>Infinity</c> hold no digit and read as what they say. A decimal keeps
/// every digit it was given or is refused: <see cref="decimal"/> rounds past its 28 or 29 digits, with no
/// word of its own.
/// </summary>
public static class TextNumber
{
    private const NumberStyles DecimalStyle = NumberStyles.Number & ~NumberStyles.AllowThousands;

    // A decimal holds any 28 digits, and a text of this length writes no more than that.
    private const int MaxDigitsAlwaysKept = 28;

    public static bool TryReadDecimal(string text, out decimal value)
        => HasDecimalForm(text, out value) && KeepsDigits(text, value);

    /// <summary>
    /// Whether a text is written as a decimal, whether or not one holds all its digits. Inference takes it,
    /// so a text with too many digits is a decimal column that refuses the cell, not a double that rounds it.
    /// </summary>
    public static bool HasDecimalForm(string text, out decimal rounded)
        => decimal.TryParse(text, DecimalStyle, CultureInfo.InvariantCulture, out rounded);

    /// <summary>
    /// Whether <paramref name="value"/> holds every digit <paramref name="number"/> writes, in plain or
    /// exponent form. Leading and trailing zeros count for nothing; a plain text too short to exceed a
    /// decimal's digits costs one comparison, where a short exponent can still reach past them.
    /// </summary>
    public static bool KeepsDigits(string number, decimal value)
        => (number.Length <= MaxDigitsAlwaysKept && number.AsSpan().IndexOfAny('e', 'E') < 0)
           || SameDigits(number, value.ToString(CultureInfo.InvariantCulture));

    public static bool TryReadDouble(string text, out double value)
        => double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value)
           && !(double.IsInfinity(value) && HoldsDigit(text));

    public static bool TryReadFloat(string text, out float value)
        => float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value)
           && !(float.IsInfinity(value) && HoldsDigit(text));

    /// <summary>
    /// Whether a number reads as a double that writes the same digits back in its shortest spelling. Every
    /// text reader's inference asks this and nothing else: a number no double rewrites is never rounded, and
    /// the 17 digits a double's own formatter leaves are not taken for a decimal they may not fit. It is
    /// judged off the text, since a decimal has already rounded a number past its own digits.
    /// </summary>
    public static bool DoubleHolds(string number, out double value)
        => TryReadDouble(number, out value) && SameDigits(number, value.ToString("R", CultureInfo.InvariantCulture));

    /// <summary>
    /// The hint of a column of texts a long does not take: a double when one holds each text, a decimal when
    /// one does not and each is written as a decimal, null otherwise, and the column stays text.
    /// </summary>
    public static string? InferHint(IReadOnlyCollection<string> samples)
    {
        if (samples.All(s => DoubleHolds(s, out _))) return "double";
        return samples.All(IsWrittenAsDecimal) ? "decimal" : null;
    }

    // A point, or an integer past a long's range. NumberStyles.Number also takes a trailing sign, and a
    // column of 5- and 12- with no point reads as codes rather than as negative numbers.
    private static bool IsWrittenAsDecimal(string text)
        => HasDecimalForm(text, out _)
           && (text.Contains('.') || decimal.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out _));

    private static bool SameDigits(string number, string other)
        => Significant(number) == Significant(other);

    private static string Significant(string number)
    {
        var mantissa = number.AsSpan();
        var exponent = mantissa.IndexOfAny('e', 'E');
        if (exponent >= 0) mantissa = mantissa[..exponent];

        var digits = new StringBuilder(mantissa.Length);
        foreach (var c in mantissa)
        {
            if (char.IsAsciiDigit(c)) digits.Append(c);
        }
        return digits.ToString().Trim('0');
    }

    private static bool HoldsDigit(string text) => text.AsSpan().IndexOfAnyInRange('0', '9') >= 0;
}
