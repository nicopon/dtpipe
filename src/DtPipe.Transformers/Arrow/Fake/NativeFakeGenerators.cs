using System.Buffers;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;

namespace DtPipe.Transformers.Arrow.Fake;

/// <summary>
/// Stateless native Arrow column generators backed by <see cref="LocaleLexicon"/> UTF-8 tables
/// and primitive Arrow builders. Uncovered paths or atypical locales return <c>null</c> from
/// <see cref="TryCreate"/> so <see cref="BogusFallbackGenerator"/> handles them seamlessly.
/// </summary>
internal static partial class NativeFakeGenerators
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
            "internet.domainname" => new DomainNameGenerator(lexicon.DomainWordsAscii, lexicon.DomainSuffixes),
            "internet.url" => new UrlGenerator(lexicon.DomainWordsAscii, lexicon.DomainSuffixes),
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
}
