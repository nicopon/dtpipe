using DtPipe.Core.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Helpers;

public class ColumnConverterFactoryTests
{
    [Fact]
    public void A_Text_With_An_Offset_Under_A_DateTime_Column_Is_Its_UTC_Reading()
    {
        var convert = ColumnConverterFactory.Build(typeof(string), typeof(DateTime), "n");

        var value = Assert.IsType<DateTime>(convert("2024-01-02T03:04:05+02:00"));

        Assert.Equal(new DateTime(2024, 1, 2, 1, 4, 5, DateTimeKind.Unspecified), value);
        Assert.Equal(DateTimeKind.Unspecified, value.Kind);
    }

    [Theory]
    [InlineData(typeof(int), "abc")]
    [InlineData(typeof(int), " ")]
    [InlineData(typeof(long), "abc")]
    [InlineData(typeof(short), "abc")]
    [InlineData(typeof(double), "abc")]
    [InlineData(typeof(float), "abc")]
    [InlineData(typeof(decimal), "abc")]
    [InlineData(typeof(bool), "abc")]
    [InlineData(typeof(DateTime), "abc")]
    [InlineData(typeof(DateTimeOffset), "abc")]
    [InlineData(typeof(Guid), "abc")]
    public void Text_That_Does_Not_Read_As_The_Target_Is_Refused_Naming_Column_And_Type(Type target, string text)
    {
        var convert = ColumnConverterFactory.Build(typeof(string), target, "n");

        var ex = Assert.Throws<InvalidOperationException>(() => convert(text));

        Assert.Contains("Column 'n'", ex.Message);
        Assert.Contains(target.Name, ex.Message);
        Assert.DoesNotContain("abc", ex.Message);
    }

    [Theory]
    [InlineData(typeof(int), "42")]
    [InlineData(typeof(long), "42")]
    [InlineData(typeof(short), "42")]
    [InlineData(typeof(double), "1.5")]
    [InlineData(typeof(float), "1.5")]
    [InlineData(typeof(decimal), "1.5")]
    [InlineData(typeof(bool), "true")]
    [InlineData(typeof(DateTime), "2024-01-02T13:45:00")]
    [InlineData(typeof(DateTimeOffset), "2024-01-02T13:45:00+01:00")]
    [InlineData(typeof(Guid), "0f8fad5b-d9cb-469f-a165-70867728950e")]
    public void Text_That_Reads_As_The_Target_Is_Converted(Type target, string text)
    {
        var convert = ColumnConverterFactory.Build(typeof(string), target, "n");

        Assert.IsType(target, convert(text));
    }

    [Theory]
    [InlineData(typeof(int), null)]
    [InlineData(typeof(int), "")]
    [InlineData(typeof(Guid), "")]
    public void Null_Or_Empty_Text_Is_Written_As_DBNull(Type target, string? text)
    {
        var convert = ColumnConverterFactory.Build(typeof(string), target, "n");

        Assert.Same(DBNull.Value, convert(text));
    }
}
