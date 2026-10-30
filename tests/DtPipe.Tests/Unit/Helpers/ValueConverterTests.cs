using DtPipe.Core.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Helpers;

public class ValueConverterTests
{
    // An integer is a count, not a point in time: it is never taken for Unix seconds or milliseconds.
    public static TheoryData<object, Type> IntegerIntoATemporalType() => new()
    {
        { 20240102L, typeof(DateTime) },
        { 20240102L, typeof(DateTimeOffset) },
        { 20240102L, typeof(DateTime?) },
        { 1_700_000_000L, typeof(DateTime) },
        { 1_700_000_000_000L, typeof(DateTime) },
        { 1_700_000_000_000L, typeof(DateTimeOffset) },
        { 20240102, typeof(DateTime) },
        { 20240102, typeof(DateTimeOffset) },
        { (short)7, typeof(DateTime) },
        { 20240102UL, typeof(DateTimeOffset) },
    };

    [Theory]
    [MemberData(nameof(IntegerIntoATemporalType))]
    public void An_Integer_Is_Refused_Into_A_Date_Or_Time_Type(object value, Type target)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => ValueConverter.ConvertValue(value, target));

        var type = (Nullable.GetUnderlyingType(target) ?? target).Name;
        Assert.Contains(type, ex.Message);
        Assert.Contains("Unix timestamp", ex.Message);
        Assert.Contains("--column-types", ex.Message);
        Assert.DoesNotContain(value.ToString()!, ex.Message);
    }

    [Fact]
    public void The_Writer_Names_The_Column_It_Refuses()
    {
        var convert = ColumnConverterFactory.Build(typeof(long), typeof(DateTime), "created");

        Assert.Equal(DBNull.Value, convert(null));
        var ex = Assert.Throws<InvalidOperationException>(() => convert(20240102L));

        Assert.Contains("Column 'created'", ex.Message);
        Assert.Contains("DateTime", ex.Message);
    }

    [Fact]
    public void A_Date_Or_Time_Text_Or_Value_Still_Converts()
    {
        var day = new DateTime(2024, 1, 2, 13, 45, 0, DateTimeKind.Utc);
        var offset = new DateTimeOffset(day);

        Assert.Equal(day, ValueConverter.ConvertValue(day, typeof(DateTime)));
        Assert.Equal(day, ValueConverter.ConvertValue("2024-01-02T13:45:00Z", typeof(DateTime)));
        Assert.Equal(offset, ValueConverter.ConvertValue("2024-01-02T13:45:00+00:00", typeof(DateTimeOffset)));
        Assert.Equal(day, ValueConverter.ConvertValue(offset, typeof(DateTime)));
        Assert.Equal(offset.UtcDateTime, ((DateTimeOffset)ValueConverter.ConvertValue(day, typeof(DateTimeOffset))).UtcDateTime);
        Assert.Equal(DBNull.Value, ValueConverter.ConvertValue(null, typeof(DateTime)));
    }

    [Fact]
    public void An_Integer_Still_Converts_To_A_Number()
    {
        Assert.Equal(42L, ValueConverter.ConvertValue(42, typeof(long)));
        Assert.Equal(42.0, ValueConverter.ConvertValue(42L, typeof(double)));
    }
}
