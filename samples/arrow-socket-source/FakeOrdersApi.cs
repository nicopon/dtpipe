namespace ArrowSocketSource;

/// <summary>An order as the external API returns it: raw, with fields that must not leave the service.</summary>
public sealed record RawOrder(
    long Id,
    string CreatedAt,
    string CustomerEmail,
    string ShippingCountry,
    int StatusCode,
    int Quantity,
    decimal UnitPrice,
    decimal DiscountPercent,
    string InternalNote);

/// <summary>
/// Stands in for the external API: a paged, deterministic list of orders, with a delay per page so
/// that a reader that leaves early is seen leaving before the last page.
/// </summary>
public sealed class FakeOrdersApi(int totalOrders, int pageSize, TimeSpan pageDelay)
{
    private static readonly string[] Countries = ["fr", "de", "es", "it", "be", "nl", "pt", "pl"];

    private int _pagesFetched;

    public int TotalOrders => totalOrders;

    /// <summary>How many pages the API has served since the process started: what a reader that left early must not make grow.</summary>
    public int PagesFetched => Volatile.Read(ref _pagesFetched);

    public async IAsyncEnumerable<IReadOnlyList<RawOrder>> FetchPagesAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct, bool delay = true)
    {
        for (var start = 0; start < totalOrders; start += pageSize)
        {
            if (delay && pageDelay > TimeSpan.Zero) await Task.Delay(pageDelay, ct);
            ct.ThrowIfCancellationRequested();
            Interlocked.Increment(ref _pagesFetched);
            var count = Math.Min(pageSize, totalOrders - start);
            var page = new List<RawOrder>(count);
            for (var i = start; i < start + count; i++) page.Add(Order(i));
            yield return page;
        }
    }

    private static RawOrder Order(int i)
    {
        // Seeded by the index, so every page, every connection and every process sees the same order.
        var random = new Random(i);
        var created = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddMinutes(i * 7 + random.Next(7));
        return new RawOrder(
            Id: 100_000 + i,
            CreatedAt: created.ToString("O"),
            CustomerEmail: $"customer{random.Next(5000)}@example.com",
            ShippingCountry: Countries[random.Next(Countries.Length)],
            StatusCode: random.Next(10),
            Quantity: 1 + random.Next(9),
            UnitPrice: Math.Round((decimal)(random.NextDouble() * 200 + 1), 2),
            DiscountPercent: random.Next(4) * 5,
            InternalNote: $"note {random.Next()}");
    }
}
