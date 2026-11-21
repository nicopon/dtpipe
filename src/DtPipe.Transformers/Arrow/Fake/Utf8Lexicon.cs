using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Bogus;
using Bogus.Bson;

namespace DtPipe.Transformers.Arrow.Fake;

/// <summary>
/// Immutable flat UTF-8 string table backed by one byte buffer and an offset index.
/// Retains zero managed <see cref="string"/> instances after construction.
/// </summary>
internal sealed class Utf8StringList
{
    private readonly byte[] _data;
    private readonly int[] _offsets;

    public int Count { get; }
    public int AverageByteLength { get; }

    private Utf8StringList(byte[] data, int[] offsets, int count)
    {
        _data = data;
        _offsets = offsets;
        Count = count;
        AverageByteLength = count == 0 ? 8 : Math.Max(4, data.Length / count);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> Get(int index)
    {
        int start = _offsets[index];
        int end = _offsets[index + 1];
        return _data.AsSpan(start, end - start);
    }

    public static Utf8StringList FromStrings(IReadOnlyList<string> items)
    {
        if (items.Count == 0)
            return new Utf8StringList([], [0], 0);

        int totalBytes = 0;
        for (int i = 0; i < items.Count; i++)
            totalBytes += Encoding.UTF8.GetByteCount(items[i]);

        var data = new byte[totalBytes];
        var offsets = new int[items.Count + 1];
        int pos = 0;
        for (int i = 0; i < items.Count; i++)
        {
            offsets[i] = pos;
            pos += Encoding.UTF8.GetBytes(items[i].AsSpan(), data.AsSpan(pos));
        }
        offsets[items.Count] = pos;
        return new Utf8StringList(data, offsets, items.Count);
    }

    public static Utf8StringList? FromBsonArray(BValue? value, bool rejectTemplates = true)
    {
        if (value is not BArray arr || arr.Count == 0)
            return null;

        var items = new List<string>(arr.Count);
        for (int i = 0; i < arr.Count; i++)
        {
            var s = arr[i]?.StringValue;
            if (string.IsNullOrEmpty(s))
                continue;
            if (rejectTemplates && (s.Contains("#{", StringComparison.Ordinal) || s.Contains("{{", StringComparison.Ordinal)))
                return null;
            items.Add(s);
        }

        return items.Count == 0 ? null : FromStrings(items);
    }
}

/// <summary>
/// Pre-compiled Bogus <c>#{token}</c> template array stored as UTF-8 segments.
/// Returns <c>null</c> from <see cref="TryCompile"/> whenever any token is unresolvable so
/// the column automatically routes to the Bogus fallback generator.
/// </summary>
internal sealed class Utf8CompiledTemplate
{
    private readonly Segment[][] _alternatives;

    private Utf8CompiledTemplate(Segment[][] alternatives)
    {
        _alternatives = alternatives;
    }

    public int AverageByteLength => 24;

    public bool TryWrite(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
    {
        var segments = _alternatives[rng.NextInt(_alternatives.Length)];
        int pos = 0;
        for (int i = 0; i < segments.Length; i++)
        {
            if (!segments[i].TryWrite(ref rng, destination[pos..], out int w))
            {
                bytesWritten = 0;
                return false;
            }
            pos += w;
        }
        bytesWritten = pos;
        return true;
    }

    public static Utf8CompiledTemplate? TryCompile(
        string category,
        string key,
        string locale,
        Utf8StringList? firstNames,
        Dictionary<string, object?> cache,
        int depth = 0)
    {
        if (depth > 3)
            return null;

        var raw = Database.Get(category, key, locale, "en");
        if (raw is not BArray arr || arr.Count == 0)
            return null;

        var alternatives = new List<Segment[]>(arr.Count);
        for (int i = 0; i < arr.Count; i++)
        {
            var pattern = arr[i]?.StringValue;
            if (string.IsNullOrEmpty(pattern) || pattern.Contains("{{", StringComparison.Ordinal))
                return null;

            var parsed = TryParseAlternative(pattern, category, locale, firstNames, cache, depth);
            if (parsed is null)
                return null;

            alternatives.Add(parsed);
        }

        return alternatives.Count == 0 ? null : new Utf8CompiledTemplate(alternatives.ToArray());
    }

    private static Segment[]? TryParseAlternative(
        string pattern,
        string currentCategory,
        string locale,
        Utf8StringList? firstNames,
        Dictionary<string, object?> cache,
        int depth)
    {
        var segments = new List<Segment>();
        int pos = 0;

        while (pos < pattern.Length)
        {
            int start = pattern.IndexOf("#{", pos, StringComparison.Ordinal);
            if (start < 0)
            {
                segments.Add(Segment.Literal(Encoding.UTF8.GetBytes(pattern[pos..])));
                break;
            }

            if (start > pos)
                segments.Add(Segment.Literal(Encoding.UTF8.GetBytes(pattern[pos..start])));

            int end = pattern.IndexOf('}', start + 2);
            if (end < 0)
                return null;

            var token = pattern[(start + 2)..end];
            var dot = token.IndexOf('.');
            string targetCat;
            string targetKey;
            if (dot > 0)
            {
                targetCat = token[..dot].ToLowerInvariant();
                targetKey = token[(dot + 1)..];
            }
            else
            {
                targetCat = currentCategory;
                targetKey = token;
            }

            var resolved = ResolveToken(targetCat, targetKey, locale, firstNames, cache, depth + 1);
            if (!resolved.HasValue)
                return null;

            segments.Add(resolved.Value);
            pos = end + 1;
        }

        return segments.ToArray();
    }

    private static Segment? ResolveToken(
        string category,
        string key,
        string locale,
        Utf8StringList? firstNames,
        Dictionary<string, object?> cache,
        int depth)
    {
        if (category == "name" && key == "first_name" && firstNames is not null)
            return Segment.Plain(firstNames);

        string cacheKey = $"{category}.{key}";
        if (cache.TryGetValue(cacheKey, out var cached))
        {
            return cached switch
            {
                Utf8StringList list => IsPatternKey(key) ? Segment.Pattern(list) : Segment.Plain(list),
                Utf8CompiledTemplate nested => Segment.Nested(nested),
                _ => null
            };
        }

        var raw = Database.Get(category, key, locale, "en");
        if (raw is not BArray arr || arr.Count == 0)
        {
            cache[cacheKey] = null;
            return null;
        }

        var plain = Utf8StringList.FromBsonArray(arr, rejectTemplates: true);
        if (plain is not null)
        {
            cache[cacheKey] = plain;
            return IsPatternKey(key) ? Segment.Pattern(plain) : Segment.Plain(plain);
        }

        var nestedTpl = TryCompile(category, key, locale, firstNames, cache, depth);
        cache[cacheKey] = nestedTpl;
        return nestedTpl is null ? null : Segment.Nested(nestedTpl);
    }

    private static bool IsPatternKey(string key) =>
        key is "building_number" or "postcode" or "formats";

    internal static bool TryExpandPattern(ReadOnlySpan<byte> pattern, ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
    {
        if (pattern.Length > destination.Length)
        {
            bytesWritten = 0;
            return false;
        }

        for (int i = 0; i < pattern.Length; i++)
        {
            byte b = pattern[i];
            destination[i] = b switch
            {
                (byte)'#' => (byte)('0' + rng.NextInt(10)),
                (byte)'!' => (byte)('2' + rng.NextInt(8)),
                (byte)'?' => (byte)('A' + rng.NextInt(26)),
                _ => b
            };
        }

        bytesWritten = pattern.Length;
        return true;
    }

    private readonly struct Segment
    {
        private readonly byte[]? _literal;
        private readonly Utf8StringList? _list;
        private readonly Utf8CompiledTemplate? _nested;
        private readonly bool _isPattern;

        private Segment(byte[]? literal, Utf8StringList? list, Utf8CompiledTemplate? nested, bool isPattern)
        {
            _literal = literal;
            _list = list;
            _nested = nested;
            _isPattern = isPattern;
        }

        public static Segment Literal(byte[] bytes) => new(bytes, null, null, false);
        public static Segment Plain(Utf8StringList list) => new(null, list, null, false);
        public static Segment Pattern(Utf8StringList list) => new(null, list, null, true);
        public static Segment Nested(Utf8CompiledTemplate nested) => new(null, null, nested, false);

        public bool TryWrite(ref SplitMix64 rng, Span<byte> destination, out int bytesWritten)
        {
            if (_literal is not null)
            {
                if (_literal.Length > destination.Length)
                {
                    bytesWritten = 0;
                    return false;
                }
                _literal.CopyTo(destination);
                bytesWritten = _literal.Length;
                return true;
            }

            if (_nested is not null)
                return _nested.TryWrite(ref rng, destination, out bytesWritten);

            if (_list is not null)
            {
                var item = _list.Get(rng.NextInt(_list.Count));
                if (_isPattern)
                    return TryExpandPattern(item, ref rng, destination, out bytesWritten);

                if (item.Length > destination.Length)
                {
                    bytesWritten = 0;
                    return false;
                }
                item.CopyTo(destination);
                bytesWritten = item.Length;
                return true;
            }

            bytesWritten = 0;
            return false;
        }
    }
}

/// <summary>
/// Shared immutable UTF-8 word/template tables extracted once per locale from <see cref="Bogus.Database"/>.
/// </summary>
internal sealed class LocaleLexicon
{
    public string Locale { get; }

    public Utf8StringList FirstNames { get; }
    public Utf8StringList LastNames { get; }
    public Utf8StringList FirstNamesAscii { get; }
    public Utf8StringList LastNamesAscii { get; }
    public Utf8StringList? NamePrefixes { get; }
    public Utf8StringList NameSuffixes { get; }
    public Utf8StringList JobDescriptors { get; }
    public Utf8StringList JobAreas { get; }
    public Utf8StringList JobTypes { get; }

    public Utf8StringList Countries { get; }
    public Utf8StringList CountryCodes { get; }
    public Utf8StringList States { get; }
    public Utf8StringList StateAbbrs { get; }
    public Utf8StringList ZipCodeFormats { get; }
    public Utf8StringList BuildingNumberFormats { get; }
    public Utf8CompiledTemplate? CityTemplate { get; }
    public Utf8CompiledTemplate? StreetNameTemplate { get; }
    public Utf8CompiledTemplate? StreetAddressTemplate { get; }

    public Utf8StringList FreeEmailDomains { get; }
    public Utf8StringList ExampleEmailDomains { get; }
    public Utf8StringList DomainSuffixes { get; }
    public Utf8StringList PhoneFormats { get; }
    public Utf8StringList LoremWords { get; }

    public Utf8StringList CompanySuffixes { get; }
    public Utf8StringList CompanyAdjectives { get; }
    public Utf8StringList CompanyNouns { get; }
    public Utf8StringList CompanyDescriptors { get; }
    public Utf8StringList CompanyBsAdjectives { get; }
    public Utf8StringList CompanyBsNouns { get; }
    public Utf8StringList CompanyBsVerbs { get; }

    public Utf8StringList CommerceColors { get; }
    public Utf8StringList CommerceDepartments { get; }
    public Utf8StringList CommerceProducts { get; }
    public Utf8StringList CommerceProductAdjectives { get; }
    public Utf8StringList CommerceProductMaterials { get; }

    public Utf8StringList FinanceAccountTypes { get; }
    public Utf8StringList DateMonths { get; }
    public Utf8StringList DateWeekdays { get; }

    public Utf8StringList VehicleManufacturers { get; }
    public Utf8StringList VehicleModels { get; }
    public Utf8StringList VehicleTypes { get; }
    public Utf8StringList VehicleFuels { get; }

    public Utf8StringList DatabaseColumns { get; }
    public Utf8StringList DatabaseTypes { get; }
    public Utf8StringList DatabaseCollations { get; }
    public Utf8StringList DatabaseEngines { get; }

    public Utf8StringList HackerAbbreviations { get; }
    public Utf8StringList HackerAdjectives { get; }
    public Utf8StringList HackerNouns { get; }
    public Utf8StringList HackerVerbs { get; }

    public Utf8StringList SystemDirectoryPaths { get; }

    private LocaleLexicon(
        string locale,
        Utf8StringList firstNames,
        Utf8StringList lastNames,
        Utf8StringList firstNamesAscii,
        Utf8StringList lastNamesAscii,
        Utf8StringList? namePrefixes,
        Utf8StringList nameSuffixes,
        Utf8StringList jobDescriptors,
        Utf8StringList jobAreas,
        Utf8StringList jobTypes,
        Utf8StringList countries,
        Utf8StringList countryCodes,
        Utf8StringList states,
        Utf8StringList stateAbbrs,
        Utf8StringList zipCodeFormats,
        Utf8StringList buildingNumberFormats,
        Utf8CompiledTemplate? cityTemplate,
        Utf8CompiledTemplate? streetNameTemplate,
        Utf8CompiledTemplate? streetAddressTemplate,
        Utf8StringList freeEmailDomains,
        Utf8StringList exampleEmailDomains,
        Utf8StringList domainSuffixes,
        Utf8StringList phoneFormats,
        Utf8StringList loremWords,
        Utf8StringList companySuffixes,
        Utf8StringList companyAdjectives,
        Utf8StringList companyNouns,
        Utf8StringList companyDescriptors,
        Utf8StringList companyBsAdjectives,
        Utf8StringList companyBsNouns,
        Utf8StringList companyBsVerbs,
        Utf8StringList commerceColors,
        Utf8StringList commerceDepartments,
        Utf8StringList commerceProducts,
        Utf8StringList commerceProductAdjectives,
        Utf8StringList commerceProductMaterials,
        Utf8StringList financeAccountTypes,
        Utf8StringList dateMonths,
        Utf8StringList dateWeekdays,
        Utf8StringList vehicleManufacturers,
        Utf8StringList vehicleModels,
        Utf8StringList vehicleTypes,
        Utf8StringList vehicleFuels,
        Utf8StringList databaseColumns,
        Utf8StringList databaseTypes,
        Utf8StringList databaseCollations,
        Utf8StringList databaseEngines,
        Utf8StringList hackerAbbreviations,
        Utf8StringList hackerAdjectives,
        Utf8StringList hackerNouns,
        Utf8StringList hackerVerbs,
        Utf8StringList systemDirectoryPaths)
    {
        Locale = locale;
        FirstNames = firstNames;
        LastNames = lastNames;
        FirstNamesAscii = firstNamesAscii;
        LastNamesAscii = lastNamesAscii;
        NamePrefixes = namePrefixes;
        NameSuffixes = nameSuffixes;
        JobDescriptors = jobDescriptors;
        JobAreas = jobAreas;
        JobTypes = jobTypes;
        Countries = countries;
        CountryCodes = countryCodes;
        States = states;
        StateAbbrs = stateAbbrs;
        ZipCodeFormats = zipCodeFormats;
        BuildingNumberFormats = buildingNumberFormats;
        CityTemplate = cityTemplate;
        StreetNameTemplate = streetNameTemplate;
        StreetAddressTemplate = streetAddressTemplate;
        FreeEmailDomains = freeEmailDomains;
        ExampleEmailDomains = exampleEmailDomains;
        DomainSuffixes = domainSuffixes;
        PhoneFormats = phoneFormats;
        LoremWords = loremWords;
        CompanySuffixes = companySuffixes;
        CompanyAdjectives = companyAdjectives;
        CompanyNouns = companyNouns;
        CompanyDescriptors = companyDescriptors;
        CompanyBsAdjectives = companyBsAdjectives;
        CompanyBsNouns = companyBsNouns;
        CompanyBsVerbs = companyBsVerbs;
        CommerceColors = commerceColors;
        CommerceDepartments = commerceDepartments;
        CommerceProducts = commerceProducts;
        CommerceProductAdjectives = commerceProductAdjectives;
        CommerceProductMaterials = commerceProductMaterials;
        FinanceAccountTypes = financeAccountTypes;
        DateMonths = dateMonths;
        DateWeekdays = dateWeekdays;
        VehicleManufacturers = vehicleManufacturers;
        VehicleModels = vehicleModels;
        VehicleTypes = vehicleTypes;
        VehicleFuels = vehicleFuels;
        DatabaseColumns = databaseColumns;
        DatabaseTypes = databaseTypes;
        DatabaseCollations = databaseCollations;
        DatabaseEngines = databaseEngines;
        HackerAbbreviations = hackerAbbreviations;
        HackerAdjectives = hackerAdjectives;
        HackerNouns = hackerNouns;
        HackerVerbs = hackerVerbs;
        SystemDirectoryPaths = systemDirectoryPaths;
    }

    public static LocaleLexicon Load(string locale, LocaleLexicon? englishFallback)
    {
        // Warm Bogus.Database for both the target locale and "en" so concurrent fallback generators
        // never contend on lazy locale initialization.
        var enObj = Database.GetLocale("en");
        var locObj = string.Equals(locale, "en", StringComparison.Ordinal) ? enObj : Database.GetLocale(locale);

        var firstNameStrings = ExtractFirstNames(locObj, locale);
        var lastNameStrings = ExtractStrings(Database.Get("name", "last_name", locale, "en"));

        var firstNames = Utf8StringList.FromStrings(firstNameStrings);
        var lastNames = Utf8StringList.FromStrings(lastNameStrings);

        var firstAsciiStrings = FoldAsciiEmailParts(firstNameStrings);
        var lastAsciiStrings = FoldAsciiEmailParts(lastNameStrings);
        var firstNamesAscii = firstAsciiStrings.Count >= 5
            ? Utf8StringList.FromStrings(firstAsciiStrings)
            : (englishFallback?.FirstNamesAscii ?? Utf8StringList.FromStrings(["alex", "sam", "chris", "pat", "taylor"]));
        var lastNamesAscii = lastAsciiStrings.Count >= 5
            ? Utf8StringList.FromStrings(lastAsciiStrings)
            : (englishFallback?.LastNamesAscii ?? Utf8StringList.FromStrings(["smith", "jones", "brown", "lee", "clark"]));

        var tplCache = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        return new LocaleLexicon(
            locale: locale,
            firstNames: firstNames,
            lastNames: lastNames,
            firstNamesAscii: firstNamesAscii,
            lastNamesAscii: lastNamesAscii,
            namePrefixes: Utf8StringList.FromBsonArray(Database.Get("name", "prefix", locale, "en"), rejectTemplates: true),
            nameSuffixes: LoadPlain(locale, "name", "suffix"),
            jobDescriptors: LoadPlain(locale, "name", "title.descriptor"),
            jobAreas: LoadPlain(locale, "name", "title.level"),
            jobTypes: LoadPlain(locale, "name", "title.job"),
            countries: LoadPlain(locale, "address", "country"),
            countryCodes: LoadPlain(locale, "address", "country_code"),
            states: LoadPlain(locale, "address", "state"),
            stateAbbrs: LoadPlain(locale, "address", "state_abbr"),
            zipCodeFormats: LoadPlain(locale, "address", "postcode"),
            buildingNumberFormats: LoadPlain(locale, "address", "building_number"),
            cityTemplate: Utf8CompiledTemplate.TryCompile("address", "city", locale, firstNames, tplCache),
            streetNameTemplate: Utf8CompiledTemplate.TryCompile("address", "street_name", locale, firstNames, tplCache),
            streetAddressTemplate: Utf8CompiledTemplate.TryCompile("address", "street_address", locale, firstNames, tplCache),
            freeEmailDomains: LoadPlain(locale, "internet", "free_email"),
            exampleEmailDomains: LoadPlain(locale, "internet", "example_email"),
            domainSuffixes: LoadPlain(locale, "internet", "domain_suffix"),
            phoneFormats: LoadPlain(locale, "phone_number", "formats"),
            loremWords: LoadPlain(locale, "lorem", "words"),
            companySuffixes: LoadPlain(locale, "company", "suffix"),
            companyAdjectives: LoadPlain(locale, "company", "adjective"),
            companyNouns: LoadPlain(locale, "company", "noun"),
            companyDescriptors: LoadPlain(locale, "company", "descriptor"),
            companyBsAdjectives: LoadPlain(locale, "company", "bs_adjective"),
            companyBsNouns: LoadPlain(locale, "company", "bs_noun"),
            companyBsVerbs: LoadPlain(locale, "company", "bs_verb"),
            commerceColors: LoadPlain(locale, "commerce", "color"),
            commerceDepartments: LoadPlain(locale, "commerce", "department"),
            commerceProducts: LoadPlain(locale, "commerce", "product_name.product"),
            commerceProductAdjectives: LoadPlain(locale, "commerce", "product_name.adjective"),
            commerceProductMaterials: LoadPlain(locale, "commerce", "product_name.material"),
            financeAccountTypes: LoadPlain(locale, "finance", "account_type"),
            dateMonths: LoadPlain(locale, "date", "month.wide"),
            dateWeekdays: LoadPlain(locale, "date", "weekday.wide"),
            vehicleManufacturers: LoadPlain(locale, "vehicle", "manufacturer"),
            vehicleModels: LoadPlain(locale, "vehicle", "model"),
            vehicleTypes: LoadPlain(locale, "vehicle", "type"),
            vehicleFuels: LoadPlain(locale, "vehicle", "fuel"),
            databaseColumns: LoadPlain(locale, "database", "column"),
            databaseTypes: LoadPlain(locale, "database", "type"),
            databaseCollations: LoadPlain(locale, "database", "collation"),
            databaseEngines: LoadPlain(locale, "database", "engine"),
            hackerAbbreviations: LoadPlain(locale, "hacker", "abbreviation"),
            hackerAdjectives: LoadPlain(locale, "hacker", "adjective"),
            hackerNouns: LoadPlain(locale, "hacker", "noun"),
            hackerVerbs: LoadPlain(locale, "hacker", "verb"),
            systemDirectoryPaths: LoadPlain(locale, "system", "directoryPaths"));
    }

    private static Utf8StringList LoadPlain(string locale, string category, string key)
    {
        var arr = Database.Get(category, key, locale, "en");
        return Utf8StringList.FromBsonArray(arr, rejectTemplates: false)
            ?? Utf8StringList.FromBsonArray(Database.Get(category, key, "en", "en"), rejectTemplates: false)
            ?? Utf8StringList.FromStrings([]);
    }

    private static List<string> ExtractFirstNames(BObject locObj, string locale)
    {
        // Locales such as "fr" define male_first_name/female_first_name without a combined
        // first_name key; check the locale object directly before falling back to "en".
        if (locObj.ContainsKey("name") && locObj["name"] is BObject nameObj)
        {
            if (nameObj.ContainsKey("first_name"))
            {
                var direct = ExtractStrings(nameObj["first_name"]);
                if (direct.Count > 0) return direct;
            }

            var combined = new List<string>();
            if (nameObj.ContainsKey("male_first_name"))
                combined.AddRange(ExtractStrings(nameObj["male_first_name"]));
            if (nameObj.ContainsKey("female_first_name"))
                combined.AddRange(ExtractStrings(nameObj["female_first_name"]));
            if (combined.Count > 0)
                return combined;
        }

        return ExtractStrings(Database.Get("name", "first_name", locale, "en"));
    }

    private static List<string> ExtractStrings(BValue? value)
    {
        var list = new List<string>();
        if (value is not BArray arr) return list;
        for (int i = 0; i < arr.Count; i++)
        {
            var s = arr[i]?.StringValue;
            if (!string.IsNullOrEmpty(s))
                list.Add(s);
        }
        return list;
    }

    private static List<string> FoldAsciiEmailParts(List<string> source)
    {
        var result = new List<string>(source.Count);
        var sb = new StringBuilder(32);

        for (int i = 0; i < source.Count; i++)
        {
            sb.Clear();
            var normalized = source[i].Normalize(NormalizationForm.FormD);
            for (int j = 0; j < normalized.Length; j++)
            {
                var c = normalized[j];
                if (CharUnicodeInfo.GetUnicodeCategory(c) == UnicodeCategory.NonSpacingMark)
                    continue;

                if (c is >= 'A' and <= 'Z')
                    sb.Append((char)(c + 32));
                else if (c is >= 'a' and <= 'z' or >= '0' and <= '9' or '_' or '-')
                    sb.Append(c);
                else if (c is ' ' or '.')
                {
                    if (sb.Length > 0 && sb[^1] != '.')
                        sb.Append('.');
                }
            }

            while (sb.Length > 0 && sb[^1] == '.')
                sb.Length--;

            if (sb.Length > 0)
                result.Add(sb.ToString());
        }

        return result;
    }
}

/// <summary>
/// Process-wide cache of immutable <see cref="LocaleLexicon"/> instances keyed by locale name.
/// </summary>
internal static class LocaleLexiconCache
{
    private static readonly ConcurrentDictionary<string, Lazy<LocaleLexicon>> Cache =
        new(StringComparer.OrdinalIgnoreCase);

    public static LocaleLexicon Get(string locale)
    {
        var english = GetCore("en", null);
        if (string.Equals(locale, "en", StringComparison.OrdinalIgnoreCase))
            return english;

        return GetCore(locale, english);
    }

    private static LocaleLexicon GetCore(string locale, LocaleLexicon? englishFallback)
    {
        var lazy = Cache.GetOrAdd(
            locale,
            static (loc, en) => new Lazy<LocaleLexicon>(
                () => LocaleLexicon.Load(loc, en),
                LazyThreadSafetyMode.ExecutionAndPublication),
            englishFallback);

        return lazy.Value;
    }
}
