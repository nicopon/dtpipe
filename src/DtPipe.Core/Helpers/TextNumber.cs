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
    /// exponent form. Leading and trailing zeros count for nothing; a text too short to exceed a decimal's
    /// digits costs one comparison.
    /// </summary>
    public static bool KeepsDigits(string number, decimal value)
        => number.Length <= MaxDigitsAlwaysKept
           || Significant(number).SequenceEqual(Significant(value.ToString(CultureInfo.InvariantCulture)));

    public static bool TryReadDouble(string text, out double value)
        => double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value)
           && !(double.IsInfinity(value) && HoldsDigit(text));

    public static bool TryReadFloat(string text, out float value)
        => float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value)
           && !(float.IsInfinity(value) && HoldsDigit(text));

    /// <summary>
    /// Whether a double keeps the digits of the decimal the same number reads as. Inference takes it to
    /// choose between the two, so a number with more digits than a double holds is not rounded.
    /// </summary>
    public static bool DoubleKeepsDigits(decimal exact, double approximate)
        => Math.Abs(approximate) < (double)decimal.MaxValue && (decimal)approximate == exact;

    /// <summary>
    /// Whether a double's shortest round-trip spelling reads back as the same decimal. A JSON number
    /// usually comes from a double's own formatter, with up to 17 digits a double carries exactly, so
    /// <see cref="DoubleKeepsDigits"/> would call it a decimal it may not fit.
    /// </summary>
    public static bool DoubleRoundTrips(decimal exact, double approximate)
        => decimal.TryParse(approximate.ToString("R", CultureInfo.InvariantCulture), NumberStyles.Float, CultureInfo.InvariantCulture, out var back)
           && back == exact;

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
