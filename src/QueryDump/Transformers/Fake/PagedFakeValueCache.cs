using Bogus;

namespace QueryDump.Transformers.Fake;

/// <summary>
/// Lazily-populated paged cache for deterministic fake data generation.
/// Pages are generated on-demand to minimize Randomizer instantiation overhead.
/// Memory is bounded by MAX_CACHED_VALUES (65536 values).
/// </summary>
public sealed class PagedFakeValueCache
{
    private const int PAGE_SIZE = 1024;
    private const int MAX_CACHED_VALUES = 65536;

    private readonly string _locale;
    private readonly Func<Faker, object?> _generator;
    private readonly uint _fakerHash;
    private readonly Dictionary<int, object?[]> _pages = [];

    public PagedFakeValueCache(string locale, Func<Faker, object?> generator, uint fakerHash)
    {
        _locale = locale;
        _generator = generator;
        _fakerHash = fakerHash;
    }

    /// <summary>
    /// Gets a deterministic fake value for the given index.
    /// Same index always returns the same value.
    /// </summary>
    public object? GetValue(int index)
    {
        // Apply modulo to limit total cached values
        // This bounds memory while maintaining determinism (same index → same value)
        var boundedIndex = ((index % MAX_CACHED_VALUES) + MAX_CACHED_VALUES) % MAX_CACHED_VALUES;
        
        var pageIndex = boundedIndex / PAGE_SIZE;
        var offset = boundedIndex % PAGE_SIZE;

        if (!_pages.TryGetValue(pageIndex, out var page))
        {
            page = GeneratePage(pageIndex);
            _pages[pageIndex] = page;
        }

        return page[offset];
    }

    private object?[] GeneratePage(int pageIndex)
    {
        // Compute a deterministic seed for this page
        // Combine fakerHash + pageIndex to ensure different pages have different seeds
        var pageSeed = unchecked((int)(_fakerHash + (uint)pageIndex * 397));
        
        var faker = new Faker(_locale) { Random = new Randomizer(pageSeed) };
        var page = new object?[PAGE_SIZE];
        
        for (int i = 0; i < PAGE_SIZE; i++)
        {
            page[i] = _generator(faker);
        }
        
        return page;
    }
}
