using System.Text;

namespace DtPipe.Transformers.Arrow.Fake;

internal static partial class NativeFakeGenerators
{
    private sealed class ConstantStringGenerator : Utf8ColumnGeneratorBase
    {
        private readonly byte[] _utf8;

        public ConstantStringGenerator(string literal)
        {
            _utf8 = Encoding.UTF8.GetBytes(literal);
        }

        protected override int EstimatedBytesPerRow => _utf8.Length;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (_utf8.Length > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }
            _utf8.CopyTo(destination);
            bytesWritten = _utf8.Length;
            return true;
        }
    }

    private sealed class Utf8ListGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _list;

        public Utf8ListGenerator(Utf8StringList list) => _list = list;

        protected override int EstimatedBytesPerRow => _list.AverageByteLength;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (_list.Count == 0)
            {
                bytesWritten = 0;
                return true;
            }

            var item = _list.Get(rng.NextInt(_list.Count));
            if (item.Length > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }
            item.CopyTo(destination);
            bytesWritten = item.Length;
            return true;
        }
    }

    private sealed class Utf8ListWithSuffixGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _list;
        private readonly byte[] _suffix;

        public Utf8ListWithSuffixGenerator(Utf8StringList list, ReadOnlySpan<byte> suffix)
        {
            _list = list;
            _suffix = suffix.ToArray();
        }

        protected override int EstimatedBytesPerRow => _list.AverageByteLength + _suffix.Length;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var item = _list.Count > 0 ? _list.Get(rng.NextInt(_list.Count)) : ReadOnlySpan<byte>.Empty;
            int total = item.Length + _suffix.Length;
            if (total > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }
            item.CopyTo(destination);
            _suffix.CopyTo(destination[item.Length..]);
            bytesWritten = total;
            return true;
        }
    }

    private sealed class Utf8TwoPartGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _a;
        private readonly Utf8StringList _b;
        private readonly byte[] _sep;

        public Utf8TwoPartGenerator(Utf8StringList a, Utf8StringList b, ReadOnlySpan<byte> sep)
        {
            _a = a;
            _b = b;
            _sep = sep.ToArray();
        }

        protected override int EstimatedBytesPerRow => _a.AverageByteLength + _sep.Length + _b.AverageByteLength;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var sa = _a.Count > 0 ? _a.Get(rng.NextInt(_a.Count)) : ReadOnlySpan<byte>.Empty;
            var sb = _b.Count > 0 ? _b.Get(rng.NextInt(_b.Count)) : ReadOnlySpan<byte>.Empty;
            int total = sa.Length + _sep.Length + sb.Length;
            if (total > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }
            sa.CopyTo(destination);
            _sep.CopyTo(destination[sa.Length..]);
            sb.CopyTo(destination[(sa.Length + _sep.Length)..]);
            bytesWritten = total;
            return true;
        }
    }

    private sealed class Utf8ThreePartGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _a;
        private readonly Utf8StringList _b;
        private readonly Utf8StringList _c;
        private readonly byte[] _sep1;
        private readonly byte[] _sep2;

        public Utf8ThreePartGenerator(Utf8StringList a, Utf8StringList b, Utf8StringList c, ReadOnlySpan<byte> sep1, ReadOnlySpan<byte> sep2)
        {
            _a = a;
            _b = b;
            _c = c;
            _sep1 = sep1.ToArray();
            _sep2 = sep2.ToArray();
        }

        protected override int EstimatedBytesPerRow => _a.AverageByteLength + _b.AverageByteLength + _c.AverageByteLength + 2;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var sa = _a.Count > 0 ? _a.Get(rng.NextInt(_a.Count)) : ReadOnlySpan<byte>.Empty;
            var sb = _b.Count > 0 ? _b.Get(rng.NextInt(_b.Count)) : ReadOnlySpan<byte>.Empty;
            var sc = _c.Count > 0 ? _c.Get(rng.NextInt(_c.Count)) : ReadOnlySpan<byte>.Empty;
            int total = sa.Length + _sep1.Length + sb.Length + _sep2.Length + sc.Length;
            if (total > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }
            int p = 0;
            sa.CopyTo(destination[p..]); p += sa.Length;
            _sep1.CopyTo(destination[p..]); p += _sep1.Length;
            sb.CopyTo(destination[p..]); p += sb.Length;
            _sep2.CopyTo(destination[p..]); p += _sep2.Length;
            sc.CopyTo(destination[p..]); p += sc.Length;
            bytesWritten = p;
            return true;
        }
    }

    private sealed class Utf8TemplateGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8CompiledTemplate _template;

        public Utf8TemplateGenerator(Utf8CompiledTemplate template) => _template = template;

        protected override int EstimatedBytesPerRow => _template.AverageByteLength;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
            => _template.TryWrite(ref rng, destination, out bytesWritten);
    }

    private sealed class Utf8PatternGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _formats;

        public Utf8PatternGenerator(Utf8StringList formats) => _formats = formats;

        protected override int EstimatedBytesPerRow => _formats.AverageByteLength;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            var pattern = _formats.Get(rng.NextInt(_formats.Count));
            return Utf8CompiledTemplate.TryExpandPattern(pattern, ref rng, destination, out bytesWritten);
        }
    }

    private sealed class FullAddressGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8CompiledTemplate _streetAddress;
        private readonly Utf8CompiledTemplate _city;
        private readonly Utf8StringList _states;
        private readonly Utf8StringList _zipFormats;

        public FullAddressGenerator(
            Utf8CompiledTemplate streetAddress,
            Utf8CompiledTemplate city,
            Utf8StringList states,
            Utf8StringList zipFormats)
        {
            _streetAddress = streetAddress;
            _city = city;
            _states = states;
            _zipFormats = zipFormats;
        }

        protected override int EstimatedBytesPerRow => 72;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            int p = 0;
            if (!_streetAddress.TryWrite(ref rng, destination[p..], out int w)) { bytesWritten = 0; return false; }
            p += w;

            if (p + 2 > destination.Length) { bytesWritten = 0; return false; }
            destination[p++] = (byte)',';
            destination[p++] = (byte)' ';

            if (!_city.TryWrite(ref rng, destination[p..], out w)) { bytesWritten = 0; return false; }
            p += w;

            if (p + 2 > destination.Length) { bytesWritten = 0; return false; }
            destination[p++] = (byte)',';
            destination[p++] = (byte)' ';

            var state = _states.Get(rng.NextInt(_states.Count));
            if (p + state.Length + 1 > destination.Length) { bytesWritten = 0; return false; }
            state.CopyTo(destination[p..]);
            p += state.Length;
            destination[p++] = (byte)' ';

            var zipPattern = _zipFormats.Get(rng.NextInt(_zipFormats.Count));
            if (!Utf8CompiledTemplate.TryExpandPattern(zipPattern, ref rng, destination[p..], out w)) { bytesWritten = 0; return false; }
            p += w;

            bytesWritten = p;
            return true;
        }
    }

    private sealed class LoremWordsGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _words;
        private readonly int _count;
        private readonly byte _separator;

        public LoremWordsGenerator(Utf8StringList words, int count, byte separator)
        {
            _words = words;
            _count = count;
            _separator = separator;
        }

        protected override int EstimatedBytesPerRow => _count * (_words.AverageByteLength + 1);

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            int p = 0;
            for (int i = 0; i < _count; i++)
            {
                if (i > 0)
                {
                    if (p >= destination.Length) { bytesWritten = 0; return false; }
                    destination[p++] = _separator;
                }
                var w = _words.Get(rng.NextInt(_words.Count));
                if (p + w.Length > destination.Length) { bytesWritten = 0; return false; }
                w.CopyTo(destination[p..]);
                p += w.Length;
            }
            bytesWritten = p;
            return true;
        }
    }

    private sealed class LoremSentenceGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _words;

        public LoremSentenceGenerator(Utf8StringList words) => _words = words;

        protected override int EstimatedBytesPerRow => 64;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
            => TryWriteSentence(_words, ref rng, destination, out bytesWritten);

        internal static bool TryWriteSentence(Utf8StringList words, ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            int wordCount = rng.NextInt(3, 11);
            int p = 0;
            for (int i = 0; i < wordCount; i++)
            {
                if (i > 0)
                {
                    if (p >= destination.Length) { bytesWritten = 0; return false; }
                    destination[p++] = (byte)' ';
                }
                var w = words.Get(rng.NextInt(words.Count));
                if (p + w.Length > destination.Length) { bytesWritten = 0; return false; }
                w.CopyTo(destination[p..]);
                if (i == 0 && w.Length > 0 && destination[p] is >= (byte)'a' and <= (byte)'z')
                    destination[p] = (byte)(destination[p] - 32);
                p += w.Length;
            }

            if (p >= destination.Length) { bytesWritten = 0; return false; }
            destination[p++] = (byte)'.';
            bytesWritten = p;
            return true;
        }
    }

    private sealed class LoremSentencesGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _words;
        private readonly int _count;

        public LoremSentencesGenerator(Utf8StringList words, int count)
        {
            _words = words;
            _count = count;
        }

        protected override int EstimatedBytesPerRow => _count * 64;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            int p = 0;
            for (int i = 0; i < _count; i++)
            {
                if (i > 0)
                {
                    if (p >= destination.Length) { bytesWritten = 0; return false; }
                    destination[p++] = (byte)' ';
                }
                if (!LoremSentenceGenerator.TryWriteSentence(_words, ref rng, destination[p..], out int w))
                {
                    bytesWritten = 0;
                    return false;
                }
                p += w;
            }
            bytesWritten = p;
            return true;
        }
    }

    private sealed class LoremParagraphGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _words;

        public LoremParagraphGenerator(Utf8StringList words) => _words = words;

        protected override int EstimatedBytesPerRow => 256;
        protected override int StackScratchSize => 1024;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
            => TryWriteParagraph(_words, ref rng, destination, out bytesWritten);

        internal static bool TryWriteParagraph(Utf8StringList words, ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            int sentences = rng.NextInt(3, 6);
            int p = 0;
            for (int i = 0; i < sentences; i++)
            {
                if (i > 0)
                {
                    if (p >= destination.Length) { bytesWritten = 0; return false; }
                    destination[p++] = (byte)' ';
                }
                if (!LoremSentenceGenerator.TryWriteSentence(words, ref rng, destination[p..], out int w))
                {
                    bytesWritten = 0;
                    return false;
                }
                p += w;
            }
            bytesWritten = p;
            return true;
        }
    }

    private sealed class LoremParagraphsGenerator : Utf8ColumnGeneratorBase
    {
        private readonly Utf8StringList _words;
        private readonly int _count;

        public LoremParagraphsGenerator(Utf8StringList words, int count)
        {
            _words = words;
            _count = count;
        }

        protected override int EstimatedBytesPerRow => _count * 256;
        protected override int StackScratchSize => 2048;

        protected override bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            int p = 0;
            for (int i = 0; i < _count; i++)
            {
                if (i > 0)
                {
                    if (p + 2 > destination.Length) { bytesWritten = 0; return false; }
                    destination[p++] = (byte)'\n';
                    destination[p++] = (byte)'\n';
                }
                if (!LoremParagraphGenerator.TryWriteParagraph(_words, ref rng, destination[p..], out int w))
                {
                    bytesWritten = 0;
                    return false;
                }
                p += w;
            }
            bytesWritten = p;
            return true;
        }
    }
}
