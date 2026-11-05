namespace DtPipe.Adapters.PostgreSQL;

/// <summary>
/// PostgreSQL's temporal values off their binary form, which is what tells <c>infinity</c> from a date.
/// Read as a <see cref="DateTime"/>, Npgsql turns <c>-infinity</c> into <see cref="DateTime.MinValue"/>,
/// which is also the real <c>0001-01-01</c>; its switch to raise instead is an <c>AppContext</c> value of
/// the process, which a library must not set for its host. Neither form has a value of its own for a
/// year outside 1 to 9999, so those are refused too.
/// </summary>
internal static class PostgresTemporal
{
    private static readonly long EpochTicks = new DateTime(2000, 1, 1).Ticks;
    private static readonly long MinMicroseconds = (DateTime.MinValue.Ticks - EpochTicks) / TimeSpan.TicksPerMicrosecond;
    private static readonly long MaxMicroseconds = (DateTime.MaxValue.Ticks - EpochTicks) / TimeSpan.TicksPerMicrosecond;
    private static readonly int EpochDayNumber = new DateOnly(2000, 1, 1).DayNumber;

    /// <summary>A <c>timestamp</c> or <c>timestamptz</c> from its microseconds since 2000-01-01, with no kind.</summary>
    internal static DateTime Timestamp(long microseconds)
    {
        if (microseconds == long.MaxValue) throw new UnreadableValue("holds infinity, which no date or time type here can hold");
        if (microseconds == long.MinValue) throw new UnreadableValue("holds -infinity, which no date or time type here can hold");
        if (microseconds < MinMicroseconds || microseconds > MaxMicroseconds) throw new UnreadableValue(OutsideYears);
        return new DateTime(EpochTicks + microseconds * TimeSpan.TicksPerMicrosecond, DateTimeKind.Unspecified);
    }

    /// <summary>A <c>date</c> from its days since 2000-01-01.</summary>
    internal static DateOnly Date(int days)
    {
        if (days == int.MaxValue) throw new UnreadableValue("holds infinity, which no date or time type here can hold");
        if (days == int.MinValue) throw new UnreadableValue("holds -infinity, which no date or time type here can hold");
        var dayNumber = (long)EpochDayNumber + days;
        if (dayNumber < DateOnly.MinValue.DayNumber || dayNumber > DateOnly.MaxValue.DayNumber) throw new UnreadableValue(OutsideYears);
        return DateOnly.FromDayNumber((int)dayNumber);
    }

    internal static DateTime?[] Timestamps(long?[] microseconds)
        => Array.ConvertAll(microseconds, m => m is { } value ? Timestamp(value) : (DateTime?)null);

    internal static DateOnly?[] Dates(int?[] days)
        => Array.ConvertAll(days, d => d is { } value ? Date(value) : (DateOnly?)null);

    private const string OutsideYears = "holds a date outside the years 1 to 9999";

    /// <summary>What a value is, for the reader that knows its column to refuse it.</summary>
    internal sealed class UnreadableValue(string problem) : Exception(problem);
}
