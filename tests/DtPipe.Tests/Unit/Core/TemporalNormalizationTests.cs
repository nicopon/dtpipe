using System;
using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// Pins the conversion rule for a zone-less <c>DateTime</c> (Kind=Unspecified — what every ADO
/// driver returns for PostgreSQL <c>timestamp</c>, MySQL <c>datetime</c>, SQL Server
/// <c>datetime2</c>): a wall clock survives a columnar hop unchanged, identically on every machine.
///
/// Assertions here are machine-independent on purpose, so they hold on a UTC CI runner — which
/// also means they cannot, alone, prove a zone dependency. tests/scripts/validate_temporal.sh
/// does that, by running the real pipeline under two TZ values.
/// </summary>
public class TemporalNormalizationTests
{
    private static readonly DateTime WallClock = new(2026, 8, 28, 9, 30, 0, DateTimeKind.Unspecified);

    [Theory]
    [InlineData("2026-08-28T09:30:00")]
    [InlineData("2026-08-28T09:30:00Z")]
    [InlineData("2026-08-28T11:30:00+02:00")]
    [InlineData(" 2026-08-28T09:30:00 ")]
    public void A_Text_Reads_As_A_Zone_Less_Wall_Clock_Whatever_It_Says(string text)
    {
        Assert.True(TemporalNormalization.TryParseWallClock(text, out var value));

        Assert.Equal(WallClock, value);
        Assert.Equal(DateTimeKind.Unspecified, value.Kind);
    }

    [Theory]
    [InlineData("2026-08-28T09:30:00", 0)]
    [InlineData("2026-08-28T09:30:00Z", 0)]
    [InlineData("2026-08-28T11:30:00+02:00", 2)]
    public void A_Text_Reads_As_An_Instant_And_Keeps_Its_Offset_Or_Takes_None(string text, int hours)
    {
        Assert.True(TemporalNormalization.TryParseInstant(text, out var value));

        Assert.Equal(WallClock, value.UtcDateTime);
        Assert.Equal(TimeSpan.FromHours(hours), value.Offset);
    }

    [Fact]
    public void Unspecified_Is_Stored_As_A_Wall_Clock_Never_As_A_Local_Instant()
    {
        var result = TemporalNormalization.ToOffset(WallClock);

        // Offset zero means no ambient zone was consulted.
        Assert.Equal(TimeSpan.Zero, result.Offset);
        // …so the ticks that reach Arrow are the ones the source actually carried.
        Assert.Equal(WallClock, result.UtcDateTime);
    }

    [Fact]
    public void Utc_Keeps_Its_Instant()
    {
        var utc = new DateTime(2026, 8, 28, 9, 30, 0, DateTimeKind.Utc);
        var result = TemporalNormalization.ToOffset(utc);

        Assert.Equal(TimeSpan.Zero, result.Offset);
        Assert.Equal(utc, result.UtcDateTime);
    }

    [Fact]
    public void Local_Keeps_Its_Instant()
    {
        // A Kind=Local value names a real instant, so converting it is correct and unchanged.
        var local = new DateTime(2026, 8, 28, 9, 30, 0, DateTimeKind.Local);
        var result = TemporalNormalization.ToOffset(local);

        Assert.Equal(TimeZoneInfo.Local.GetUtcOffset(local), result.Offset);
        Assert.Equal(local.ToUniversalTime(), result.UtcDateTime);
    }

    [Fact]
    public void ToWallClock_Is_The_Exact_Inverse_Of_ToOffset()
    {
        // The two halves are inverses by construction; that is why they share a class.
        var restored = TemporalNormalization.ToWallClock(TemporalNormalization.ToOffset(WallClock));

        Assert.Equal(WallClock, restored);
        // Zone-less in, zone-less out — a wall clock never acquires a zone on the way through.
        Assert.Equal(DateTimeKind.Unspecified, restored.Kind);
    }

    [Fact]
    public void ToWallClock_Reads_The_Utc_Component_Not_The_Local_One()
    {
        // Values read out of an Arrow timestamp array always carry a zero offset, so this is moot
        // today; it is pinned so the helper stays correct if one with a real offset ever arrives.
        var offsetValue = new DateTimeOffset(2026, 8, 28, 11, 30, 0, TimeSpan.FromHours(2));

        Assert.Equal(new DateTime(2026, 8, 28, 9, 30, 0), TemporalNormalization.ToWallClock(offsetValue));
    }

    [Theory]
    [InlineData(null)]    // zone-less Arrow timestamp
    [InlineData("UTC")]   // zone-aware Arrow timestamp
    public void Round_Trip_Through_TimestampArray_Preserves_The_Wall_Clock(string? timezone)
    {
        // The Arrow type's timezone is irrelevant to Builder.Append(DateTime) — only DateTime.Kind
        // matters, so a zone-aware column needs the rule as much as a zone-less one. Both pinned.
        var builder = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, timezone));
        builder.Append(TemporalNormalization.ToOffset(WallClock));

        var readBack = builder.Build().GetTimestamp(0);

        Assert.NotNull(readBack);
        Assert.Equal(WallClock, readBack!.Value.UtcDateTime);
    }

    /// <summary>
    /// Date32/Date64 are zone-insensitive: they keep only the date part and consult no ambient
    /// zone, so no conversion is applied to them. Pinned so the property is not lost if their
    /// append path is ever rerouted through instant-based logic.
    /// </summary>
    [Theory]
    [InlineData(0, 0)]
    [InlineData(23, 30)]   // late in the day: where a westward local offset would roll the date back
    public void Date32_Keeps_The_Calendar_Day(int hour, int minute)
    {
        var value = new DateTime(2026, 8, 28, hour, minute, 0, DateTimeKind.Unspecified);
        var builder = new Date32Array.Builder();
        builder.Append(value);

        Assert.Equal(new DateTime(2026, 8, 28), builder.Build().GetDateTime(0));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(23, 30)]
    public void Date64_Keeps_The_Calendar_Day(int hour, int minute)
    {
        var value = new DateTime(2026, 8, 28, hour, minute, 0, DateTimeKind.Unspecified);
        var builder = new Date64Array.Builder();
        builder.Append(value);

        Assert.Equal(new DateTime(2026, 8, 28), builder.Build().GetDateTime(0));
    }
}
