using System.Buffers.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DtPipe.Transformers.Arrow.Fake;

internal static partial class NativeFakeGenerators
{
    private sealed class EmailGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _firstNames;
        private readonly Utf8StringList _lastNames;
        private readonly Utf8StringList _domains;

        public EmailGenerator(Utf8StringList firstNames, Utf8StringList lastNames, Utf8StringList domains)
        {
            _firstNames = firstNames;
            _lastNames = lastNames;
            _domains = domains;
        }

        protected override int EstimatedBytesPerRow => 26;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var first = _firstNames.Get(rng.NextInt(_firstNames.Count));
            var last = _lastNames.Get(rng.NextInt(_lastNames.Count));
            var domain = _domains.Get(rng.NextInt(_domains.Count));
            int style = rng.NextInt(4);

            int maxLen = first.Length + last.Length + domain.Length + 6;
            if (maxLen > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }

            int p = 0;
            first.CopyTo(destination[p..]); p += first.Length;

            switch (style)
            {
                case 0:
                    destination[p++] = (byte)'.';
                    last.CopyTo(destination[p..]); p += last.Length;
                    break;
                case 1:
                    destination[p++] = (byte)'_';
                    last.CopyTo(destination[p..]); p += last.Length;
                    break;
                case 2:
                    int num = rng.NextInt(100);
                    destination[p++] = (byte)('0' + num / 10);
                    destination[p++] = (byte)('0' + num % 10);
                    break;
                default:
                    destination[p++] = (byte)'.';
                    last.CopyTo(destination[p..]); p += last.Length;
                    int num2 = rng.NextInt(100);
                    destination[p++] = (byte)('0' + num2 / 10);
                    destination[p++] = (byte)('0' + num2 % 10);
                    break;
            }

            destination[p++] = (byte)'@';
            domain.CopyTo(destination[p..]); p += domain.Length;
            bytesWritten = p;
            return true;
        }
    }

    private sealed class UserNameGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _firstNames;
        private readonly Utf8StringList _lastNames;

        public UserNameGenerator(Utf8StringList firstNames, Utf8StringList lastNames)
        {
            _firstNames = firstNames;
            _lastNames = lastNames;
        }

        protected override int EstimatedBytesPerRow => 16;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var first = _firstNames.Get(rng.NextInt(_firstNames.Count));
            var last = _lastNames.Get(rng.NextInt(_lastNames.Count));
            int style = rng.NextInt(3);

            if (first.Length + last.Length + 4 > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }

            int p = 0;
            first.CopyTo(destination[p..]); p += first.Length;
            if (style == 0)
            {
                destination[p++] = (byte)'.';
                last.CopyTo(destination[p..]); p += last.Length;
            }
            else if (style == 1)
            {
                destination[p++] = (byte)'_';
                last.CopyTo(destination[p..]); p += last.Length;
            }
            else
            {
                int num = rng.NextInt(100);
                destination[p++] = (byte)('0' + num / 10);
                destination[p++] = (byte)('0' + num % 10);
            }

            bytesWritten = p;
            return true;
        }
    }

    private sealed class DomainNameGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _words;
        private readonly Utf8StringList _suffixes;

        public DomainNameGenerator(Utf8StringList words, Utf8StringList suffixes)
        {
            _words = words;
            _suffixes = suffixes;
        }

        protected override int EstimatedBytesPerRow => 16;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var w = _words.Get(rng.NextInt(_words.Count));
            var s = _suffixes.Get(rng.NextInt(_suffixes.Count));
            int total = w.Length + 1 + s.Length;
            if (total > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }

            w.CopyTo(destination);
            destination[w.Length] = (byte)'.';
            s.CopyTo(destination[(w.Length + 1)..]);
            bytesWritten = total;
            return true;
        }
    }

    private sealed class UrlGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _words;
        private readonly Utf8StringList _suffixes;

        public UrlGenerator(Utf8StringList words, Utf8StringList suffixes)
        {
            _words = words;
            _suffixes = suffixes;
        }

        protected override int EstimatedBytesPerRow => 24;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var w = _words.Get(rng.NextInt(_words.Count));
            var s = _suffixes.Get(rng.NextInt(_suffixes.Count));
            ReadOnlySpan<byte> scheme = (rng.NextUInt64() & 1) == 0 ? "https://"u8 : "http://"u8;
            int total = scheme.Length + w.Length + 1 + s.Length;
            if (total > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }

            int p = 0;
            scheme.CopyTo(destination[p..]); p += scheme.Length;
            w.CopyTo(destination[p..]); p += w.Length;
            destination[p++] = (byte)'.';
            s.CopyTo(destination[p..]); p += s.Length;
            bytesWritten = p;
            return true;
        }
    }

    private sealed class Ipv4Generator : Utf8ColumnGeneratorBase
    {
        protected override int EstimatedBytesPerRow => 15;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 15) { bytesWritten = 0; return false; }
            ulong v = rng.NextUInt64();
            int p = 0;
            for (int octet = 0; octet < 4; octet++)
            {
                if (octet > 0) destination[p++] = (byte)'.';
                byte val = (byte)((v >> (octet * 8)) & 0xFF);
                if (!Utf8Formatter.TryFormat(val, destination[p..], out int w)) { bytesWritten = 0; return false; }
                p += w;
            }
            bytesWritten = p;
            return true;
        }
    }

    private sealed class Ipv6Generator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> Hex => "0123456789abcdef"u8;

        protected override int EstimatedBytesPerRow => 39;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 39) { bytesWritten = 0; return false; }
            ulong a = rng.NextUInt64();
            ulong b = rng.NextUInt64();
            int p = 0;
            for (int g = 0; g < 8; g++)
            {
                if (g > 0) destination[p++] = (byte)':';
                uint seg = (uint)(((g < 4 ? a : b) >> ((g & 3) * 16)) & 0xFFFF);
                destination[p++] = Hex[(int)((seg >> 12) & 0xF)];
                destination[p++] = Hex[(int)((seg >> 8) & 0xF)];
                destination[p++] = Hex[(int)((seg >> 4) & 0xF)];
                destination[p++] = Hex[(int)(seg & 0xF)];
            }
            bytesWritten = p;
            return true;
        }
    }

    private sealed class MacGenerator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> Hex => "0123456789abcdef"u8;

        protected override int EstimatedBytesPerRow => 17;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 17) { bytesWritten = 0; return false; }
            ulong v = rng.NextUInt64();
            int p = 0;
            for (int i = 0; i < 6; i++)
            {
                if (i > 0) destination[p++] = (byte)':';
                byte b = (byte)((v >> (i * 8)) & 0xFF);
                destination[p++] = Hex[b >> 4];
                destination[p++] = Hex[b & 0xF];
            }
            bytesWritten = p;
            return true;
        }
    }

    private sealed class PasswordGenerator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> Alphabet => "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-"u8;

        protected override int EstimatedBytesPerRow => 10;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 10) { bytesWritten = 0; return false; }
            for (int i = 0; i < 10; i++)
                destination[i] = Alphabet[rng.NextInt(Alphabet.Length)];
            bytesWritten = 10;
            return true;
        }
    }

    private sealed class UserAgentGenerator : Utf8ColumnGeneratorBase
    {
        protected override int EstimatedBytesPerRow => 120;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            int variant = rng.NextInt(4);
            int p = 0;

            switch (variant)
            {
                case 0:
                    {
                        var prefix = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"u8;
                        var suffix = " Safari/537.36"u8;
                        if (destination.Length < prefix.Length + suffix.Length + 20) { bytesWritten = 0; return false; }
                        prefix.CopyTo(destination); p += prefix.Length;
                        Utf8Formatter.TryFormat(rng.NextInt(110, 135), destination[p..], out int w); p += w;
                        destination[p++] = (byte)'.'; destination[p++] = (byte)'0'; destination[p++] = (byte)'.';
                        Utf8Formatter.TryFormat(rng.NextInt(4000, 7000), destination[p..], out w); p += w;
                        destination[p++] = (byte)'.';
                        Utf8Formatter.TryFormat(rng.NextInt(10, 200), destination[p..], out w); p += w;
                        suffix.CopyTo(destination[p..]); p += suffix.Length;
                        break;
                    }
                case 1:
                    {
                        var prefix = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:"u8;
                        var mid = ".0) Gecko/20100101 Firefox/"u8;
                        if (destination.Length < prefix.Length + mid.Length + 12) { bytesWritten = 0; return false; }
                        int major = rng.NextInt(110, 135);
                        prefix.CopyTo(destination); p += prefix.Length;
                        Utf8Formatter.TryFormat(major, destination[p..], out int w); p += w;
                        mid.CopyTo(destination[p..]); p += mid.Length;
                        Utf8Formatter.TryFormat(major, destination[p..], out w); p += w;
                        destination[p++] = (byte)'.'; destination[p++] = (byte)'0';
                        break;
                    }
                case 2:
                    {
                        var prefix = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/"u8;
                        var suffix = " Safari/605.1.15"u8;
                        if (destination.Length < prefix.Length + suffix.Length + 8) { bytesWritten = 0; return false; }
                        prefix.CopyTo(destination); p += prefix.Length;
                        Utf8Formatter.TryFormat(rng.NextInt(15, 19), destination[p..], out int w); p += w;
                        destination[p++] = (byte)'.';
                        destination[p++] = (byte)('0' + rng.NextInt(7));
                        suffix.CopyTo(destination[p..]); p += suffix.Length;
                        break;
                    }
                default:
                    {
                        var prefix = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"u8;
                        var suffix = " Safari/537.36"u8;
                        if (destination.Length < prefix.Length + suffix.Length + 20) { bytesWritten = 0; return false; }
                        prefix.CopyTo(destination); p += prefix.Length;
                        Utf8Formatter.TryFormat(rng.NextInt(110, 135), destination[p..], out int w); p += w;
                        destination[p++] = (byte)'.'; destination[p++] = (byte)'0'; destination[p++] = (byte)'.';
                        Utf8Formatter.TryFormat(rng.NextInt(4000, 7000), destination[p..], out w); p += w;
                        destination[p++] = (byte)'.';
                        Utf8Formatter.TryFormat(rng.NextInt(10, 200), destination[p..], out w); p += w;
                        suffix.CopyTo(destination[p..]); p += suffix.Length;
                        break;
                    }
            }

            bytesWritten = p;
            return true;
        }
    }

    private sealed class PriceStringGenerator : Utf8ColumnGeneratorBase
    {
        protected override int EstimatedBytesPerRow => 7;

        public override IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            if (targetField.DataType is Decimal128Type decType)
            {
                var b = new Decimal128Array.Builder(decType);
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append(rng.NextInt(100, 100_001) / 100m);
                }
                return b.Build();
            }

            if (targetField.DataType is DoubleType)
            {
                var b = new DoubleArray.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append(rng.NextInt(100, 100_001) / 100.0);
                }
                return b.Build();
            }

            return base.GenerateColumn(targetField, rowCount, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex, skipNullSource);
        }

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 8) { bytesWritten = 0; return false; }
            int cents = rng.NextInt(100, 100_001);
            int whole = cents / 100;
            int frac = cents % 100;
            if (!Utf8Formatter.TryFormat(whole, destination, out int p)) { bytesWritten = 0; return false; }
            destination[p++] = (byte)'.';
            destination[p++] = (byte)('0' + frac / 10);
            destination[p++] = (byte)('0' + frac % 10);
            bytesWritten = p;
            return true;
        }
    }

    private sealed class EanGenerator : Utf8ColumnGeneratorBase
    {
        private readonly int _length;

        public EanGenerator(int length) => _length = length;

        protected override int EstimatedBytesPerRow => _length;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < _length) { bytesWritten = 0; return false; }
            int payloadLen = _length - 1;
            int sum = 0;
            for (int i = 0; i < payloadLen; i++)
            {
                int d = rng.NextInt(10);
                destination[i] = (byte)('0' + d);
                // Standard EAN-8 / EAN-13 check-digit weights alternate 3 and 1 from right to left.
                int weight = ((payloadLen - 1 - i) & 1) == 0 ? 3 : 1;
                sum += d * weight;
            }
            int check = (10 - (sum % 10)) % 10;
            destination[payloadLen] = (byte)('0' + check);
            bytesWritten = _length;
            return true;
        }
    }

    private sealed class DigitsGenerator : Utf8ColumnGeneratorBase
    {
        private readonly int _length;

        public DigitsGenerator(int length) => _length = length;

        protected override int EstimatedBytesPerRow => _length;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < _length) { bytesWritten = 0; return false; }
            for (int i = 0; i < _length; i++)
                destination[i] = (byte)('0' + rng.NextInt(10));
            bytesWritten = _length;
            return true;
        }
    }

    private sealed class HexStringGenerator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> Hex => "0123456789abcdef"u8;
        private readonly int _length;

        public HexStringGenerator(int length) => _length = length;

        protected override int EstimatedBytesPerRow => _length;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < _length) { bytesWritten = 0; return false; }
            for (int i = 0; i < _length; i += 16)
            {
                ulong r = rng.NextUInt64();
                for (int k = 0; k < 16 && i + k < _length; k++)
                    destination[i + k] = Hex[(int)((r >> (k * 4)) & 0xF)];
            }
            bytesWritten = _length;
            return true;
        }
    }

    private sealed class AlphaNumericGenerator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> Chars => "0123456789abcdefghijklmnopqrstuvwxyz"u8;
        private readonly int _length;

        public AlphaNumericGenerator(int length) => _length = length;

        protected override int EstimatedBytesPerRow => _length;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < _length) { bytesWritten = 0; return false; }
            for (int i = 0; i < _length; i++)
                destination[i] = Chars[rng.NextInt(Chars.Length)];
            bytesWritten = _length;
            return true;
        }
    }

    private sealed class VinGenerator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> VinChars => "0123456789ABCDEFGHJKLMNPRSTUVWXYZ"u8;

        protected override int EstimatedBytesPerRow => 17;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 17) { bytesWritten = 0; return false; }
            for (int i = 0; i < 17; i++)
                destination[i] = VinChars[rng.NextInt(VinChars.Length)];
            bytesWritten = 17;
            return true;
        }
    }

    private sealed class SemverGenerator : Utf8ColumnGeneratorBase
    {
        protected override int EstimatedBytesPerRow => 6;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 6) { bytesWritten = 0; return false; }
            destination[0] = (byte)('0' + rng.NextInt(10));
            destination[1] = (byte)'.';
            destination[2] = (byte)('0' + rng.NextInt(10));
            destination[3] = (byte)'.';
            destination[4] = (byte)('0' + rng.NextInt(10));
            bytesWritten = 5;
            return true;
        }
    }

    private sealed class IbanGenerator : Utf8ColumnGeneratorBase
    {
        private readonly struct IbanSpec(byte c0, byte c1, int bbanDigits)
        {
            public readonly byte C0 = c0;
            public readonly byte C1 = c1;
            public readonly int BbanDigits = bbanDigits;
        }

        private static readonly IbanSpec[] Specs =
        [
            new((byte)'F', (byte)'R', 23),
            new((byte)'D', (byte)'E', 18),
            new((byte)'G', (byte)'B', 18),
            new((byte)'E', (byte)'S', 20),
            new((byte)'I', (byte)'T', 23),
            new((byte)'N', (byte)'L', 14),
            new((byte)'B', (byte)'E', 12),
            new((byte)'C', (byte)'H', 17),
            new((byte)'P', (byte)'T', 21),
            new((byte)'A', (byte)'T', 16),
            new((byte)'I', (byte)'E', 18),
            new((byte)'L', (byte)'U', 16),
            new((byte)'P', (byte)'L', 24),
            new((byte)'S', (byte)'E', 20),
            new((byte)'D', (byte)'K', 14),
            new((byte)'N', (byte)'O', 11),
            new((byte)'F', (byte)'I', 14),
            new((byte)'C', (byte)'Z', 20)
        ];

        protected override int EstimatedBytesPerRow => 28;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var spec = Specs[rng.NextInt(Specs.Length)];
            int total = 4 + spec.BbanDigits;
            if (destination.Length < total) { bytesWritten = 0; return false; }

            destination[0] = spec.C0;
            destination[1] = spec.C1;

            int rem = 0;
            for (int i = 0; i < spec.BbanDigits; i++)
            {
                int d = rng.NextInt(10);
                destination[4 + i] = (byte)('0' + d);
                rem = (rem * 10 + d) % 97;
            }

            // ISO 13616 Mod-97: append numeric country code letters (A=10..Z=35) and "00".
            rem = (rem * 100 + (spec.C0 - 'A' + 10)) % 97;
            rem = (rem * 100 + (spec.C1 - 'A' + 10)) % 97;
            rem = (rem * 100) % 97;
            int check = 98 - rem;

            destination[2] = (byte)('0' + check / 10);
            destination[3] = (byte)('0' + check % 10);

            bytesWritten = total;
            return true;
        }
    }

    private sealed class BicGenerator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> AlphaNum => "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"u8;
        private readonly byte[] _countryBytes;

        public BicGenerator(Utf8StringList countryCodes)
        {
            var valid = new List<byte>(countryCodes.Count * 2);
            for (int i = 0; i < countryCodes.Count; i++)
            {
                var c = countryCodes.Get(i);
                if (c.Length == 2 && c[0] is >= (byte)'A' and <= (byte)'Z' && c[1] is >= (byte)'A' and <= (byte)'Z')
                {
                    valid.Add(c[0]);
                    valid.Add(c[1]);
                }
            }
            _countryBytes = valid.Count >= 2 ? valid.ToArray() : [(byte)'U', (byte)'S'];
        }

        protected override int EstimatedBytesPerRow => 8;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 8) { bytesWritten = 0; return false; }
            for (int i = 0; i < 4; i++)
                destination[i] = (byte)('A' + rng.NextInt(26));

            int cIdx = rng.NextInt(_countryBytes.Length / 2) * 2;
            destination[4] = _countryBytes[cIdx];
            destination[5] = _countryBytes[cIdx + 1];
            destination[6] = AlphaNum[rng.NextInt(AlphaNum.Length)];
            destination[7] = AlphaNum[rng.NextInt(AlphaNum.Length)];
            bytesWritten = 8;
            return true;
        }
    }

    private sealed class CreditCardNumberGenerator : Utf8ColumnGeneratorBase
    {
        protected override int EstimatedBytesPerRow => 16;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 16) { bytesWritten = 0; return false; }

            int startIdx;
            if ((rng.NextUInt64() & 1) == 0)
            {
                destination[0] = (byte)'4';
                startIdx = 1;
            }
            else
            {
                destination[0] = (byte)'5';
                destination[1] = (byte)('1' + rng.NextInt(5));
                startIdx = 2;
            }

            for (int i = startIdx; i < 15; i++)
                destination[i] = (byte)('0' + rng.NextInt(10));

            // Luhn check digit at index 15: even positions (0, 2, ..., 14) are doubled.
            int sum = 0;
            for (int i = 0; i < 15; i++)
            {
                int d = destination[i] - '0';
                if ((i & 1) == 0)
                {
                    d *= 2;
                    if (d > 9) d -= 9;
                }
                sum += d;
            }

            int check = (10 - (sum % 10)) % 10;
            destination[15] = (byte)('0' + check);
            bytesWritten = 16;
            return true;
        }
    }

    private sealed class EthereumAddressGenerator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> Hex => "0123456789abcdef"u8;
        protected override int EstimatedBytesPerRow => 42;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 42) { bytesWritten = 0; return false; }
            destination[0] = (byte)'0';
            destination[1] = (byte)'x';
            for (int i = 2; i < 42; i += 16)
            {
                ulong r = rng.NextUInt64();
                for (int k = 0; k < 16 && i + k < 42; k++)
                    destination[i + k] = Hex[(int)((r >> (k * 4)) & 0xF)];
            }
            bytesWritten = 42;
            return true;
        }
    }

    private sealed class BitcoinAddressGenerator : Utf8ColumnGeneratorBase
    {
        private static ReadOnlySpan<byte> Base58 => "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"u8;
        protected override int EstimatedBytesPerRow => 34;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (destination.Length < 34) { bytesWritten = 0; return false; }
            destination[0] = (byte)'1';
            for (int i = 1; i < 34; i++)
                destination[i] = Base58[rng.NextInt(Base58.Length)];
            bytesWritten = 34;
            return true;
        }
    }
}
