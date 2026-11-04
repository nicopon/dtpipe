using DtPipe.Adapters.SqlServer;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.SqlServer;

// SqlClient reports a date column as DateTime; the reader announces the date it holds, so a date
// copied to a date column compares equal to the type the target reads for it.
public class SqlServerReaderDateColumnTests
{
    [Theory]
    [InlineData(typeof(DateTime), "date", typeof(DateOnly))]
    [InlineData(typeof(DateTime), "datetime2", typeof(DateTime))]
    [InlineData(typeof(DateTime), "datetime", typeof(DateTime))]
    [InlineData(typeof(DateTime), "smalldatetime", typeof(DateTime))]
    [InlineData(typeof(TimeSpan), "time", typeof(TimeOnly))]
    public void The_Reader_Announces_A_Date_Or_Time_Column_As_One(Type driverType, string dataTypeName, Type expected)
        => Assert.Equal(expected, SqlServerReader.AnnouncedClrType(driverType, dataTypeName));

    [Fact]
    public void The_Reader_And_The_Target_Read_A_Date_Column_As_One_Type()
        => Assert.Equal(
            SqlServerTypeConverter.Instance.MapFromProviderType("date"),
            SqlServerReader.AnnouncedClrType(typeof(DateTime), "date"));
}
