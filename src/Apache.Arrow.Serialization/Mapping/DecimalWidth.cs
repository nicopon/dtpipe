using System;
using Apache.Arrow.Types;

namespace Apache.Arrow.Serialization.Mapping;

/// <summary>
/// The width of a decimal column and how a value of it reads, whichever road it takes: at the scale the
/// column declares, or bare when it declares none. Arrow stores one scale per column, so a column that
/// declares none cannot give back the scale each value was written at, and a read path that kept it
/// would disagree with the one that went through Arrow.
/// </summary>
public sealed class DecimalWidth
{
    // Dividing by one at the largest scale leaves the smallest scale that keeps the value exact.
    private const decimal One = 1.0000000000000000000000000000m;
    private const int MaxDigitsOfADecimal = 28;

    // One at each scale: multiplying by it pads, and never rounds, since the value does not change.
    private static readonly decimal[] Ones = BuildOnes();

    /// <summary>The width of a column that declares none, which <see cref="ArrowTypeMap.DefaultDecimal"/> marks.</summary>
    public static DecimalWidth Undeclared { get; } = new(ArrowTypeMap.DefaultDecimalType.Precision, ArrowTypeMap.DefaultDecimalType.Scale, declared: false);

    private readonly decimal? _integerLimit;

    private DecimalWidth(int precision, int scale, bool declared)
    {
        Precision = precision;
        Scale = scale;
        Declared = declared;
        _integerLimit = IntegerLimit(precision, scale);
    }

    public int Precision { get; }
    public int Scale { get; }

    /// <summary>Whether the source said this width, rather than the default standing in.</summary>
    public bool Declared { get; }

    /// <summary>
    /// The width of what a source declares. Anything Arrow cannot express (no precision, one past 38, a
    /// scale outside it) is no declaration, and holds what the narrower form could.
    /// </summary>
    public static DecimalWidth Of(int? precision, int? scale)
        => precision is int p && p >= 1 && p <= ArrowTypeMap.DefaultDecimalType.Precision && scale is int s && s >= 0 && s <= p
            ? new DecimalWidth(p, s, declared: true)
            : Undeclared;

    /// <summary>The width a Decimal128 field carries; its undeclared mark says the width is the default.</summary>
    public static DecimalWidth Of(Field field)
        => field.DataType is Decimal128Type d && !ArrowTypeMap.IsUndeclaredDecimal(field)
            ? Of(d.Precision, d.Scale)
            : Undeclared;

    /// <summary>
    /// How a reader reads <paramref name="value"/>: false when a column of this width has no form for it
    /// (the two reasons a Decimal128 builder refuses one), so that the road avoiding Arrow refuses it too.
    /// Trailing zeros alone are never a reason.
    /// </summary>
    public bool TryRead(decimal value, out decimal read)
    {
        var bare = Bare(value);
        if (bare.Scale > Scale || (_integerLimit is { } limit && Math.Abs(bare) >= limit))
        {
            read = default;
            return false;
        }

        read = Declared ? AtScale(bare, Scale) : bare;
        return true;
    }

    /// <summary>The value with no trailing zeros.</summary>
    public static decimal Bare(decimal value) => value.Scale == 0 ? value : value / One;

    /// <summary>Whether a column of <paramref name="scale"/> decimals keeps every digit of the value.</summary>
    public static bool Keeps(decimal value, int scale) => value.Scale <= scale || Bare(value).Scale <= scale;

    /// <summary>
    /// The value a builder of <paramref name="scale"/> decimals takes: it refuses more decimals than its
    /// own, trailing zeros included, so those go first.
    /// </summary>
    public static decimal ForBuilder(decimal value, int scale) => value.Scale > scale ? Bare(value) : value;

    // At most the digits a decimal holds, which is also how far an Arrow reading of the column goes. A
    // product that is zero keeps no scale, where an Arrow reading of zero does.
    private static decimal AtScale(decimal value, int scale)
    {
        var missing = scale - value.Scale;
        if (missing <= 0) return value;
        return value == 0m
            ? new decimal(0, 0, 0, false, (byte)Math.Min(scale, MaxDigitsOfADecimal))
            : value * Ones[Math.Min(missing, MaxDigitsOfADecimal)];
    }

    private static decimal[] BuildOnes()
    {
        var ones = new decimal[MaxDigitsOfADecimal + 1];
        ones[0] = 1m;
        for (var i = 1; i < ones.Length; i++) ones[i] = ones[i - 1] * 1.0m;
        return ones;
    }

    // What a Decimal128 builder takes: it counts the digits of the value it is handed, which a decimal
    // can only exceed under 29 of them, and past that the bits of the padded value run out. Null when
    // no decimal reaches the limit.
    private static decimal? IntegerLimit(int precision, int scale)
    {
        if (precision <= MaxDigitsOfADecimal)
        {
            var bound = 1m;
            for (var i = 0; i < precision - scale; i++) bound *= 10m;
            return bound;
        }

        var factor = Int128.One;
        for (var i = 0; i < scale; i++) factor *= 10;
        var limit = Int128.MaxValue / factor;
        return limit > (Int128)decimal.MaxValue ? null : (decimal)limit;
    }
}
