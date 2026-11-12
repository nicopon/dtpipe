using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace ArrowSocketSource;

/// <summary>
/// The business rules applied before any row leaves the service, and the fixed Arrow schema the rows
/// leave in. The schema never depends on the data: a node pins the contract of a source brick, and a
/// schema inferred from a page would change it from one read to the next.
/// </summary>
public static class OrdersTransform
{
    private static readonly Decimal128Type Money = new(18, 2);
    private static readonly TimestampType Utc = new(TimeUnit.Microsecond, "UTC");

    public static readonly Schema Schema = new Schema.Builder()
        .Field(f => f.Name("order_id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("ordered_at").DataType(Utc).Nullable(false))
        .Field(f => f.Name("country").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("status").DataType(StringType.Default).Nullable(false))
        .Field(f => f.Name("total").DataType(Money).Nullable(false))
        .Build();

    /// <summary>Test orders (status code 9) never leave the service.</summary>
    public static bool IsKept(RawOrder order) => order.StatusCode != 9;

    private static string Status(int code) => code switch
    {
        0 or 1 or 2 => "pending",
        3 or 4 or 5 => "shipped",
        6 or 7 => "delivered",
        _ => "cancelled",
    };

    private static decimal Total(RawOrder o) =>
        Math.Round(o.Quantity * o.UnitPrice * (1 - o.DiscountPercent / 100m), 2, MidpointRounding.AwayFromZero);

    /// <summary>The orders of the external API, one batch per page, after the business rules.</summary>
    public static async IAsyncEnumerable<RecordBatch> ReadAsync(
        FakeOrdersApi api, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var page in api.FetchPagesAsync(ct)) yield return ToBatch(page);
    }

    /// <summary>The same orders, until the external API fails after <paramref name="pages"/> pages: what the probe makes a failing source of.</summary>
    public static async IAsyncEnumerable<RecordBatch> ReadThenFailAsync(
        FakeOrdersApi api, int pages, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var served = 0;
        await foreach (var page in api.FetchPagesAsync(ct))
        {
            if (served++ == pages) throw new HttpRequestException("the orders API answered 503");
            yield return ToBatch(page);
        }
    }

    /// <summary>One page of the external API as one batch: personal data and internal notes dropped, the total computed.</summary>
    public static RecordBatch ToBatch(IReadOnlyList<RawOrder> page)
    {
        var ids = new Int64Array.Builder();
        var orderedAt = new TimestampArray.Builder(Utc);
        var country = new StringArray.Builder();
        var status = new StringArray.Builder();
        var total = new Decimal128Array.Builder(Money);
        var rows = 0;
        foreach (var o in page)
        {
            if (!IsKept(o)) continue;
            ids.Append(o.Id);
            orderedAt.Append(DateTimeOffset.Parse(o.CreatedAt, CultureInfo.InvariantCulture));
            country.Append(o.ShippingCountry.ToUpperInvariant());
            status.Append(Status(o.StatusCode));
            total.Append(Total(o));
            rows++;
        }
        return new RecordBatch(Schema, [ids.Build(), orderedAt.Build(), country.Build(), status.Build(), total.Build()], rows);
    }
}
