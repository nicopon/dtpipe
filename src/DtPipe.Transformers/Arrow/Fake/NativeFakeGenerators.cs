using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Transformers.Arrow.Fake;

/// <summary>
/// Stateless native Arrow column generators backed by <see cref="LocaleLexicon"/> UTF-8 tables
/// and primitive Arrow builders. Uncovered paths or atypical locales return <c>null</c> from
/// <see cref="TryCreate"/> so <see cref="BogusFallbackGenerator"/> handles them seamlessly.
/// </summary>
internal static class NativeFakeGenerators
{
    public static IFakeColumnGenerator? TryCreate(string basePath, LocaleLexicon lexicon, Func<DateTime> referenceDateProvider)
    {
        return basePath.ToLowerInvariant() switch
        {
            // Name
            "name.firstname" => new Utf8ListGenerator(lexicon.FirstNames),
            "name.lastname" => new Utf8ListGenerator(lexicon.LastNames),
            "name.fullname" => new Utf8TwoPartGenerator(lexicon.FirstNames, lexicon.LastNames, " "u8),
            "name.prefix" => lexicon.NamePrefixes is { Count: > 0 } ? new Utf8ListGenerator(lexicon.NamePrefixes) : null,
            "name.suffix" => lexicon.NameSuffixes.Count > 0 ? new Utf8ListGenerator(lexicon.NameSuffixes) : null,
            "name.jobtitle" => new Utf8ThreePartGenerator(lexicon.JobDescriptors, lexicon.JobAreas, lexicon.JobTypes, " "u8, " "u8),
            "name.jobdescriptor" => new Utf8ListGenerator(lexicon.JobDescriptors),
            "name.jobarea" => new Utf8ListGenerator(lexicon.JobAreas),
            "name.jobtype" => new Utf8ListGenerator(lexicon.JobTypes),

            // Address
            "address.city" => lexicon.CityTemplate is not null ? new Utf8TemplateGenerator(lexicon.CityTemplate) : null,
            "address.streetname" => lexicon.StreetNameTemplate is not null ? new Utf8TemplateGenerator(lexicon.StreetNameTemplate) : null,
            "address.streetaddress" => lexicon.StreetAddressTemplate is not null ? new Utf8TemplateGenerator(lexicon.StreetAddressTemplate) : null,
            "address.zipcode" => lexicon.ZipCodeFormats.Count > 0 ? new Utf8PatternGenerator(lexicon.ZipCodeFormats) : null,
            "address.country" => new Utf8ListGenerator(lexicon.Countries),
            "address.countrycode" => new Utf8ListGenerator(lexicon.CountryCodes),
            "address.state" => new Utf8ListGenerator(lexicon.States),
            "address.stateabbr" => new Utf8ListGenerator(lexicon.StateAbbrs),
            "address.buildingnumber" => lexicon.BuildingNumberFormats.Count > 0 ? new Utf8PatternGenerator(lexicon.BuildingNumberFormats) : null,
            "address.fulladdress" => lexicon.StreetAddressTemplate is not null && lexicon.CityTemplate is not null && lexicon.States.Count > 0 && lexicon.ZipCodeFormats.Count > 0
                ? new FullAddressGenerator(lexicon.StreetAddressTemplate, lexicon.CityTemplate, lexicon.States, lexicon.ZipCodeFormats)
                : null,
            "address.latitude" => new GeoCoordGenerator(isLatitude: true),
            "address.longitude" => new GeoCoordGenerator(isLatitude: false),

            // Internet
            "internet.email" => new EmailGenerator(lexicon.FirstNamesAscii, lexicon.LastNamesAscii, lexicon.FreeEmailDomains),
            "internet.exampleemail" => new EmailGenerator(lexicon.FirstNamesAscii, lexicon.LastNamesAscii, lexicon.ExampleEmailDomains),
            "internet.username" => new UserNameGenerator(lexicon.FirstNamesAscii, lexicon.LastNamesAscii),
            "internet.domainname" => new DomainNameGenerator(lexicon.LoremWords, lexicon.DomainSuffixes),
            "internet.url" => new UrlGenerator(lexicon.LoremWords, lexicon.DomainSuffixes),
            "internet.ip" => new Ipv4Generator(),
            "internet.ipv6" => new Ipv6Generator(),
            "internet.mac" => new MacGenerator(),
            "internet.password" => new PasswordGenerator(),
            "internet.useragent" => new UserAgentGenerator(),

            // Phone
            "phone.phonenumber" => lexicon.PhoneFormats.Count > 0 ? new Utf8PatternGenerator(lexicon.PhoneFormats) : null,

            // Company
            "company.companyname" => new Utf8TwoPartGenerator(lexicon.LastNames, lexicon.CompanySuffixes, " "u8),
            "company.catchphrase" => new Utf8ThreePartGenerator(lexicon.CompanyAdjectives, lexicon.CompanyDescriptors, lexicon.CompanyNouns, " "u8, " "u8),
            "company.bs" => new Utf8ThreePartGenerator(lexicon.CompanyBsVerbs, lexicon.CompanyBsAdjectives, lexicon.CompanyBsNouns, " "u8, " "u8),
            "company.companysuffix" => new Utf8ListGenerator(lexicon.CompanySuffixes),

            // Commerce
            "commerce.productname" => new Utf8ThreePartGenerator(lexicon.CommerceProductAdjectives, lexicon.CommerceProductMaterials, lexicon.CommerceProducts, " "u8, " "u8),
            "commerce.price" => new PriceStringGenerator(),
            "commerce.department" => new Utf8ListGenerator(lexicon.CommerceDepartments),
            "commerce.product" => new Utf8ListGenerator(lexicon.CommerceProducts),
            "commerce.color" => new Utf8ListGenerator(lexicon.CommerceColors),
            "commerce.ean8" => new EanGenerator(length: 8),
            "commerce.ean13" => new EanGenerator(length: 13),

            // Finance
            "finance.account" => new DigitsGenerator(8),
            "finance.accountname" => new Utf8ListWithSuffixGenerator(lexicon.FinanceAccountTypes, " Account"u8),
            "finance.amount" => new AmountDecimalGenerator(),
            "finance.iban" => new IbanGenerator(),
            "finance.bic" => new BicGenerator(lexicon.CountryCodes),
            "finance.bitcoinaddress" => new BitcoinAddressGenerator(),
            "finance.ethereumaddress" => new EthereumAddressGenerator(),
            "finance.creditcardnumber" => new CreditCardNumberGenerator(),
            "finance.creditcardcvv" => new DigitsGenerator(3),

            // Lorem
            "lorem.word" => new Utf8ListGenerator(lexicon.LoremWords),
            "lorem.words" => new LoremWordsGenerator(lexicon.LoremWords, 3, (byte)' '),
            "lorem.sentence" => new LoremSentenceGenerator(lexicon.LoremWords),
            "lorem.sentences" => new LoremSentencesGenerator(lexicon.LoremWords, 3),
            "lorem.paragraph" => new LoremParagraphGenerator(lexicon.LoremWords),
            "lorem.paragraphs" => new LoremParagraphsGenerator(lexicon.LoremWords, 3),
            "lorem.text" => new LoremParagraphGenerator(lexicon.LoremWords),
            "lorem.slug" => new LoremWordsGenerator(lexicon.LoremWords, 3, (byte)'-'),

            // Date
            "date.past" => new DateRangeGenerator(TimeSpan.FromDays(365), past: true, referenceDateProvider),
            "date.future" => new DateRangeGenerator(TimeSpan.FromDays(365), past: false, referenceDateProvider),
            "date.recent" => new DateRangeGenerator(TimeSpan.FromDays(1), past: true, referenceDateProvider),
            "date.soon" => new DateRangeGenerator(TimeSpan.FromDays(1), past: false, referenceDateProvider),
            "date.month" => new Utf8ListGenerator(lexicon.DateMonths),
            "date.weekday" => new Utf8ListGenerator(lexicon.DateWeekdays),

            // Random
            "random.number" => new RandomNumberGenerator(),
            "random.guid" => new UuidGenerator(),
            "random.uuid" => new UuidGenerator(),
            "random.bool" => new RandomBoolGenerator(),
            "random.word" => new Utf8ListGenerator(lexicon.LoremWords),
            "random.words" => new LoremWordsGenerator(lexicon.LoremWords, 3, (byte)' '),
            "random.hash" => new HexStringGenerator(40),
            "random.alphanumeric" => new AlphaNumericGenerator(10),

            // Vehicle
            "vehicle.vin" => new VinGenerator(),
            "vehicle.manufacturer" => new Utf8ListGenerator(lexicon.VehicleManufacturers),
            "vehicle.model" => new Utf8ListGenerator(lexicon.VehicleModels),
            "vehicle.type" => new Utf8ListGenerator(lexicon.VehicleTypes),
            "vehicle.fuel" => new Utf8ListGenerator(lexicon.VehicleFuels),

            // Hacker
            "hacker.abbreviation" => new Utf8ListGenerator(lexicon.HackerAbbreviations),
            "hacker.adjective" => new Utf8ListGenerator(lexicon.HackerAdjectives),
            "hacker.noun" => new Utf8ListGenerator(lexicon.HackerNouns),
            "hacker.verb" => new Utf8ListGenerator(lexicon.HackerVerbs),

            // System
            "system.directorypath" => new Utf8ListGenerator(lexicon.SystemDirectoryPaths),
            "system.semver" => new SemverGenerator(),

            // Database
            "database.column" => new Utf8ListGenerator(lexicon.DatabaseColumns),
            "database.type" => new Utf8ListGenerator(lexicon.DatabaseTypes),
            "database.collation" => new Utf8ListGenerator(lexicon.DatabaseCollations),
            "database.engine" => new Utf8ListGenerator(lexicon.DatabaseEngines),

            _ => null
        };
    }

    internal static IFakeColumnGenerator CreateLiteralConstant(string literal) => new ConstantStringGenerator(literal);

    private abstract class Utf8ColumnGeneratorBase : IFakeColumnGenerator
    {
        protected virtual int EstimatedBytesPerRow => 24;
        protected virtual int StackScratchSize => 512;

        public Type NativeClrType => typeof(string);

        protected abstract bool TryFormatUtf8(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten);

        public virtual IArrowArray GenerateColumn(
            Field targetField,
            int rowCount,
            ReadOnlySpan<ulong> rowKeys,
            ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode,
            ulong colSalt,
            ulong globalSeed,
            long batchStartRowIndex,
            IArrowArray? skipNullSource)
        {
            if (targetField.DataType is StringType)
            {
                var builder = new Utf8ArrowColumnBuilder(rowCount, EstimatedBytesPerRow);
                Span<byte> stackBuf = stackalloc byte[StackScratchSize];
                byte[]? heapBuf = null;

                try
                {
                    for (int i = 0; i < rowCount; i++)
                    {
                        if (skipNullSource is not null && skipNullSource.IsNull(i))
                        {
                            builder.AppendNull();
                            continue;
                        }

                        var cellSeed = FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex);
                        var rng = new SplitMix64(cellSeed);

                        if (TryFormatUtf8(ref rng, stackBuf, out int written))
                        {
                            builder.Append(stackBuf[..written]);
                            continue;
                        }

                        int size = heapBuf?.Length ?? 2048;
                        while (true)
                        {
                            if (heapBuf is null || heapBuf.Length < size)
                            {
                                if (heapBuf is not null) ArrayPool<byte>.Shared.Return(heapBuf);
                                heapBuf = ArrayPool<byte>.Shared.Rent(size);
                            }

                            var retryRng = new SplitMix64(cellSeed);
                            if (TryFormatUtf8(ref retryRng, heapBuf, out written))
                            {
                                builder.Append(heapBuf.AsSpan(0, written));
                                break;
                            }
                            size *= 2;
                        }
                    }

                    return builder.Build();
                }
                finally
                {
                    if (heapBuf is not null)
                        ArrayPool<byte>.Shared.Return(heapBuf);
                }
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            Span<byte> fallbackStack = stackalloc byte[StackScratchSize];
            byte[]? fallbackHeap = null;

            try
            {
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i))
                    {
                        appender(null);
                        continue;
                    }

                    var cellSeed = FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex);
                    var rng = new SplitMix64(cellSeed);

                    if (TryFormatUtf8(ref rng, fallbackStack, out int written))
                    {
                        appender(Encoding.UTF8.GetString(fallbackStack[..written]));
                        continue;
                    }

                    int size = fallbackHeap?.Length ?? 2048;
                    while (true)
                    {
                        if (fallbackHeap is null || fallbackHeap.Length < size)
                        {
                            if (fallbackHeap is not null) ArrayPool<byte>.Shared.Return(fallbackHeap);
                            fallbackHeap = ArrayPool<byte>.Shared.Rent(size);
                        }

                        var retryRng = new SplitMix64(cellSeed);
                        if (TryFormatUtf8(ref retryRng, fallbackHeap, out written))
                        {
                            appender(Encoding.UTF8.GetString(fallbackHeap.AsSpan(0, written)));
                            break;
                        }
                        size *= 2;
                    }
                }

                return ArrowTypeMapper.BuildArray(genericBuilder);
            }
            finally
            {
                if (fallbackHeap is not null)
                    ArrayPool<byte>.Shared.Return(fallbackHeap);
            }
        }
    }

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
            if (w.Length + s.Length + 1 > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }

            int p = 0;
            for (int i = 0; i < w.Length; i++)
            {
                byte b = w[i];
                if (b is >= (byte)'A' and <= (byte)'Z') b = (byte)(b + 32);
                if (b is >= (byte)'a' and <= (byte)'z' or >= (byte)'0' and <= (byte)'9' or (byte)'-')
                    destination[p++] = b;
            }
            if (p == 0) destination[p++] = (byte)'a';
            destination[p++] = (byte)'.';
            s.CopyTo(destination[p..]); p += s.Length;
            bytesWritten = p;
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
            if (10 + w.Length + s.Length > destination.Length)
            {
                bytesWritten = 0;
                return false;
            }

            ReadOnlySpan<byte> scheme = (rng.NextUInt64() & 1) == 0 ? "https://"u8 : "http://"u8;
            scheme.CopyTo(destination);
            int p = scheme.Length;
            for (int i = 0; i < w.Length; i++)
            {
                byte b = w[i];
                if (b is >= (byte)'A' and <= (byte)'Z') b = (byte)(b + 32);
                if (b is >= (byte)'a' and <= (byte)'z' or >= (byte)'0' and <= (byte)'9' or (byte)'-')
                    destination[p++] = b;
            }
            if (p == scheme.Length) destination[p++] = (byte)'a';
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
                {
                    destination[i + k] = Hex[(int)((r >> (k * 4)) & 0xF)];
                }
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

    private sealed class RandomNumberGenerator : IFakeColumnGenerator
    {
        public Type NativeClrType => typeof(int);

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            switch (targetField.DataType)
            {
                case Int32Type:
                    {
                        var b = new Int32Array.Builder();
                        b.Reserve(rowCount);
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                            var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                            b.Append(rng.NextInt(0, 1_000_001));
                        }
                        return b.Build();
                    }
                case Int64Type:
                    {
                        var b = new Int64Array.Builder();
                        b.Reserve(rowCount);
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                            var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                            b.Append((long)rng.NextInt(0, 1_000_001));
                        }
                        return b.Build();
                    }
                case StringType:
                    {
                        var b = new Utf8ArrowColumnBuilder(rowCount, 7);
                        Span<byte> buf = stackalloc byte[16];
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                            var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                            Utf8Formatter.TryFormat(rng.NextInt(0, 1_000_001), buf, out int w);
                            b.Append(buf[..w]);
                        }
                        return b.Build();
                    }
                default:
                    {
                        var b = ArrowTypeMapper.CreateBuilder(targetField.DataType);
                        var appender = ArrowTypeMapper.ResolveAppender(b);
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                            var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                            appender(rng.NextInt(0, 1_000_001));
                        }
                        return ArrowTypeMapper.BuildArray(b);
                    }
            }
        }
    }

    private sealed class RandomBoolGenerator : IFakeColumnGenerator
    {
        public Type NativeClrType => typeof(bool);

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            if (targetField.DataType is BooleanType)
            {
                var b = new BooleanArray.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append((rng.NextUInt64() & 1) != 0);
                }
                return b.Build();
            }

            if (targetField.DataType is StringType)
            {
                var b = new Utf8ArrowColumnBuilder(rowCount, 5);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append((rng.NextUInt64() & 1) != 0 ? "True"u8 : "False"u8);
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                appender((rng.NextUInt64() & 1) != 0);
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }
    }

    private sealed class UuidGenerator : IFakeColumnGenerator
    {
        private static ReadOnlySpan<byte> Hex => "0123456789abcdef"u8;
        public Type NativeClrType => typeof(Guid);

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            if (targetField.DataType is FixedSizeBinaryType { ByteWidth: 16 })
            {
                var b = new FixedSizeBinaryArrayBuilder(16);
                b.Reserve(rowCount);
                Span<byte> bytes = stackalloc byte[16];
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    WriteUuidV4Bytes(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex), bytes);
                    b.Append(bytes);
                }
                return b.Build();
            }

            if (targetField.DataType is StringType)
            {
                var b = new Utf8ArrowColumnBuilder(rowCount, 36);
                Span<byte> raw = stackalloc byte[16];
                Span<byte> text = stackalloc byte[36];
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    WriteUuidV4Bytes(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex), raw);
                    FormatUuidAscii(raw, text);
                    b.Append(text);
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            Span<byte> tmp = stackalloc byte[16];
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                WriteUuidV4Bytes(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex), tmp);
                appender(ArrowTypeMap.FromArrowUuidBytes(tmp));
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }

        private static void WriteUuidV4Bytes(ulong seed, Span<byte> dest)
        {
            var rng = new SplitMix64(seed);
            BinaryPrimitives.WriteUInt64BigEndian(dest[..8], rng.NextUInt64());
            BinaryPrimitives.WriteUInt64BigEndian(dest[8..], rng.NextUInt64());
            dest[6] = (byte)((dest[6] & 0x0F) | 0x40);
            dest[8] = (byte)((dest[8] & 0x3F) | 0x80);
        }

        private static void FormatUuidAscii(ReadOnlySpan<byte> raw, Span<byte> dest)
        {
            int p = 0;
            for (int i = 0; i < 16; i++)
            {
                if (i is 4 or 6 or 8 or 10)
                    dest[p++] = (byte)'-';
                byte b = raw[i];
                dest[p++] = Hex[b >> 4];
                dest[p++] = Hex[b & 0xF];
            }
        }
    }

    private sealed class GeoCoordGenerator : IFakeColumnGenerator
    {
        private readonly bool _isLatitude;
        public Type NativeClrType => typeof(double);

        public GeoCoordGenerator(bool isLatitude) => _isLatitude = isLatitude;

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            double span = _isLatitude ? 180.0 : 360.0;
            double offset = _isLatitude ? -90.0 : -180.0;

            if (targetField.DataType is DoubleType)
            {
                var b = new DoubleArray.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append(Math.Round(rng.NextDouble() * span + offset, 4));
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                appender(Math.Round(rng.NextDouble() * span + offset, 4));
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }
    }

    private sealed class AmountDecimalGenerator : IFakeColumnGenerator
    {
        public Type NativeClrType => typeof(decimal);

        public IArrowArray GenerateColumn(
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
                    decimal val = rng.NextInt(1, 100_001) / 100m;
                    b.Append(val);
                }
                return b.Build();
            }

            if (targetField.DataType is StringType)
            {
                var b = new Utf8ArrowColumnBuilder(rowCount, 7);
                Span<byte> buf = stackalloc byte[16];
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    int cents = rng.NextInt(1, 100_001);
                    Utf8Formatter.TryFormat(cents / 100, buf, out int p);
                    buf[p++] = (byte)'.';
                    int frac = cents % 100;
                    buf[p++] = (byte)('0' + frac / 10);
                    buf[p++] = (byte)('0' + frac % 10);
                    b.Append(buf[..p]);
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                var rng = new SplitMix64(FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                appender(rng.NextInt(1, 100_001) / 100m);
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }
    }

    private sealed class DateRangeGenerator : IFakeColumnGenerator
    {
        private readonly long _maxSeconds;
        private readonly bool _past;
        private readonly Func<DateTime> _referenceDateProvider;

        public Type NativeClrType => typeof(DateTime);

        public DateRangeGenerator(TimeSpan span, bool past, Func<DateTime> referenceDateProvider)
        {
            _maxSeconds = Math.Max(1, (long)span.TotalSeconds);
            _past = past;
            _referenceDateProvider = referenceDateProvider;
        }

        public IArrowArray GenerateColumn(
            Field targetField, int rowCount, ReadOnlySpan<ulong> rowKeys, ReadOnlySpan<bool> hasRowKey,
            bool deterministicMode, ulong colSalt, ulong globalSeed, long batchStartRowIndex, IArrowArray? skipNullSource)
        {
            var refDt = _referenceDateProvider();
            long refTicks = refDt.Ticks - (refDt.Ticks % TimeSpan.TicksPerSecond);

            if (targetField.DataType is TimestampType tsType)
            {
                var b = new TimestampArray.Builder(tsType);
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    long ticks = SampleTicks(refTicks, FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    // Construct offset-zero DateTimeOffset directly so wall-clock DateTime values never shift by local TZ.
                    b.Append(new DateTimeOffset(ticks, TimeSpan.Zero));
                }
                return b.Build();
            }

            if (targetField.DataType is Date32Type)
            {
                var b = new Date32Array.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    long ticks = SampleTicks(refTicks, FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append(new DateTime(ticks, DateTimeKind.Unspecified));
                }
                return b.Build();
            }

            if (targetField.DataType is Date64Type)
            {
                var b = new Date64Array.Builder();
                b.Reserve(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    if (skipNullSource is not null && skipNullSource.IsNull(i)) { b.AppendNull(); continue; }
                    long ticks = SampleTicks(refTicks, FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                    b.Append(new DateTime(ticks, DateTimeKind.Unspecified));
                }
                return b.Build();
            }

            var genericBuilder = ArrowTypeMapper.CreateBuilder(targetField.DataType);
            var appender = ArrowTypeMapper.ResolveAppender(genericBuilder);
            for (int i = 0; i < rowCount; i++)
            {
                if (skipNullSource is not null && skipNullSource.IsNull(i)) { appender(null); continue; }
                long ticks = SampleTicks(refTicks, FakeCellSeed.Compute(i, rowKeys, hasRowKey, deterministicMode, colSalt, globalSeed, batchStartRowIndex));
                appender(new DateTime(ticks, DateTimeKind.Unspecified));
            }
            return ArrowTypeMapper.BuildArray(genericBuilder);
        }

        private long SampleTicks(long refTicks, ulong seed)
        {
            var rng = new SplitMix64(seed);
            long deltaSec = 1 + rng.NextInt64(_maxSeconds);
            long deltaTicks = deltaSec * TimeSpan.TicksPerSecond;
            return _past ? refTicks - deltaTicks : refTicks + deltaTicks;
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
