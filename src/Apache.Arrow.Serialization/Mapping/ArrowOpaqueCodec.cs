using System;
using System.Globalization;
using System.Numerics;

namespace Apache.Arrow.Serialization.Mapping;

/// <summary>
/// Decodes the payload of the canonical <c>arrow.opaque</c> extension type, which a producer
/// uses for a type Arrow has no form for. Its metadata names the vendor and the type
/// (<c>{"type_name":"hugeint","vendor_name":"DuckDB"}</c>), so the payload is self-describing and
/// one table covers every vendor rather than one branch per type.
/// </summary>
/// <remarks>
/// Every layout here was read off real values before being written: an opaque payload has no
/// specification to consult, and a decode guessed from a type name produces plausible wrong
/// numbers rather than an error. A vendor/type pair absent from the table keeps its raw bytes.
/// </remarks>
public static class ArrowOpaqueCodec
{
    /// <summary>
    /// Renders an opaque payload as its exact text form, or null when the pair is unknown.
    /// </summary>
    /// <remarks>
    /// Text, not a numeric CLR type: no CLR type holds a 128-bit integer or a zone-qualified
    /// time-of-day, and an exact decimal string is what the row-mode DuckDB reader already
    /// produces for these — so the two read paths agree instead of offering a third answer.
    /// </remarks>
    public static string? TryDecode(string vendorName, string typeName, ReadOnlySpan<byte> payload)
    {
        if (!string.Equals(vendorName, "DuckDB", StringComparison.OrdinalIgnoreCase))
            return null;

        return typeName.ToLowerInvariant() switch
        {
            "hugeint" => new BigInteger(payload, isUnsigned: false, isBigEndian: false).ToString(CultureInfo.InvariantCulture),
            "uhugeint" => new BigInteger(payload, isUnsigned: true, isBigEndian: false).ToString(CultureInfo.InvariantCulture),
            "time_tz" => DecodeTimeTz(payload),
            "bit" => DecodeBit(payload),
            "bignum" => DecodeBignum(payload),
            _ => null
        };
    }

    /// <summary>Whether a vendor/type pair has a text form, used to type the column.</summary>
    public static bool CanDecode(string vendorName, string typeName)
        => string.Equals(vendorName, "DuckDB", StringComparison.OrdinalIgnoreCase)
           && typeName.ToLowerInvariant() is "hugeint" or "uhugeint" or "time_tz" or "bit" or "bignum";

    // 64 bits little-endian: microseconds since midnight in the upper 40, and an offset in the
    // low 24 stored as (MaxOffsetSeconds - actual), so a larger stored value is a westward zone.
    private const int MaxOffsetSeconds = 57599; // ±15:59:59, the range DuckDB accepts

    private static string? DecodeTimeTz(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != 8) return null;

        var raw = BitConverter.ToUInt64(payload);
        var micros = (long)(raw >> 24);
        var offsetSeconds = MaxOffsetSeconds - (int)(raw & 0xFFFFFF);

        var time = TimeSpan.FromTicks(micros * (TimeSpan.TicksPerMillisecond / 1000));
        var offset = TimeSpan.FromSeconds(offsetSeconds);
        var sign = offsetSeconds < 0 ? '-' : '+';
        var abs = offset.Duration();

        return string.Create(CultureInfo.InvariantCulture,
            $"{time.Hours:D2}:{time.Minutes:D2}:{time.Seconds:D2}{sign}{abs.Hours:D2}:{abs.Minutes:D2}");
    }

    // First byte is the count of padding bits; the bit string is the trailing
    // (8 * dataLength - padding) bits of the remaining bytes, most significant first.
    private static string? DecodeBit(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 1) return null;

        int padding = payload[0];
        var data = payload[1..];
        var bitCount = data.Length * 8 - padding;
        if (bitCount < 0) return null;

        var chars = new char[bitCount];
        for (var i = 0; i < bitCount; i++)
        {
            var bit = i + padding;
            chars[i] = (data[bit / 8] & (1 << (7 - bit % 8))) != 0 ? '1' : '0';
        }
        return new string(chars);
    }

    // Three header bytes then a big-endian magnitude. The top bit of the first byte is the sign
    // (set = positive) and the remaining 23 header bits hold the magnitude length; a negative
    // value stores every byte, header included, bitwise complemented.
    private static string? DecodeBignum(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 3) return null;

        var positive = (payload[0] & 0x80) != 0;

        Span<byte> bytes = payload.Length <= 256 ? stackalloc byte[payload.Length] : new byte[payload.Length];
        for (var i = 0; i < payload.Length; i++)
            bytes[i] = positive ? payload[i] : (byte)~payload[i];

        var length = ((bytes[0] & 0x7F) << 16) | (bytes[1] << 8) | bytes[2];
        if (length < 0 || 3 + length > bytes.Length) return null;

        var magnitude = new BigInteger(bytes.Slice(3, length), isUnsigned: true, isBigEndian: true);
        return (positive ? magnitude : -magnitude).ToString(CultureInfo.InvariantCulture);
    }
}
