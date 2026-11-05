using DtPipe.Adapters.PostgreSQL;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.PostgreSQL;

/// <summary>
/// The decoding of PostgreSQL's binary temporal forms, which the reader does itself to tell infinity from
/// a date. What the server sends for them is played by <c>validate_type_conformance.sh</c>.
/// </summary>
public class PostgresTemporalTests
{
    private static readonly DateTime Epoch = new(2000, 1, 1);

    private static long Microseconds(DateTime value) => (value - Epoch).Ticks / TimeSpan.TicksPerMicrosecond;

    private static int Days(DateOnly value) => value.DayNumber - DateOnly.FromDateTime(Epoch).DayNumber;

    [Theory]
    [InlineData(2024, 1, 2, 3, 4, 5)]
    [InlineData(1, 1, 1, 0, 0, 0)]
    [InlineData(1999, 12, 31, 23, 59, 59)]
    [InlineData(9999, 12, 31, 23, 59, 59)]
    public void A_Timestamp_In_Range_Reads_As_A_Wall_Clock(int year, int month, int day, int hour, int minute, int second)
    {
        var expected = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Unspecified);

        var value = PostgresTemporal.Timestamp(Microseconds(expected));

        Assert.Equal(expected, value);
        Assert.Equal(DateTimeKind.Unspecified, value.Kind);
    }

    [Fact]
    public void A_Timestamp_Keeps_Its_Microseconds()
    {
        var expected = new DateTime(2024, 1, 2, 3, 4, 5).AddTicks(1234560);

        Assert.Equal(expected, PostgresTemporal.Timestamp(Microseconds(expected)));
    }

    [Theory]
    [InlineData(long.MaxValue, "holds infinity")]
    [InlineData(long.MinValue, "holds -infinity")]
    [InlineData(9_000_000_000_000_000_000L, "outside the years")]
    [InlineData(-63_082_281_600_000_001L, "outside the years")]
    public void A_Timestamp_No_DateTime_Holds_Is_Refused(long microseconds, string problem)
    {
        var refusal = Assert.Throws<PostgresTemporal.UnreadableValue>(() => PostgresTemporal.Timestamp(microseconds));

        Assert.Contains(problem, refusal.Message);
    }

    [Theory]
    [InlineData(2024, 1, 2)]
    [InlineData(1, 1, 1)]
    [InlineData(9999, 12, 31)]
    public void A_Date_In_Range_Reads_As_That_Date(int year, int month, int day)
    {
        var expected = new DateOnly(year, month, day);

        Assert.Equal(expected, PostgresTemporal.Date(Days(expected)));
    }

    [Theory]
    [InlineData(int.MaxValue, "holds infinity")]
    [InlineData(int.MinValue, "holds -infinity")]
    [InlineData(3_000_000, "outside the years")]
    [InlineData(-800_000, "outside the years")]
    public void A_Date_No_DateOnly_Holds_Is_Refused(int days, string problem)
    {
        var refusal = Assert.Throws<PostgresTemporal.UnreadableValue>(() => PostgresTemporal.Date(days));

        Assert.Contains(problem, refusal.Message);
    }

    [Fact]
    public void An_Array_Keeps_Its_Nulls_And_Refuses_An_Infinite_Element()
    {
        var day = new DateTime(2024, 1, 2);

        Assert.Equal(new DateTime?[] { day, null }, PostgresTemporal.Timestamps([Microseconds(day), null]));
        Assert.Equal(new DateOnly?[] { new(2024, 1, 2), null }, PostgresTemporal.Dates([Days(new DateOnly(2024, 1, 2)), null]));
        Assert.Throws<PostgresTemporal.UnreadableValue>(() => PostgresTemporal.Timestamps([long.MaxValue]));
        Assert.Throws<PostgresTemporal.UnreadableValue>(() => PostgresTemporal.Dates([int.MinValue]));
    }
}
