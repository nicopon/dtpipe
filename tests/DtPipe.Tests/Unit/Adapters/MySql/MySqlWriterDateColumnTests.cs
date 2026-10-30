using System.Data;
using DtPipe.Adapters.MySql;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.MySql;

// The INSERT path stores each cell in a DataTable column of the buffer type before it binds it, so a
// value is checked by storing it there, which is where a mismatched type fails.
public class MySqlWriterDateColumnTests
{
    private static object? Store(Type bufferType, object? value)
    {
        using var table = new DataTable();
        table.Columns.Add("n", bufferType);
        var row = table.NewRow();
        row[0] = value;
        table.Rows.Add(row);
        return table.Rows[0][0];
    }

    [Fact]
    public void A_Date_Source_Is_Stored_As_A_Date()
    {
        var (bufferType, convert) = MySqlDataWriter.BufferColumn("date", typeof(DateTime), typeof(DateOnly), "n");

        Assert.Equal(new DateOnly(2024, 1, 2), Store(bufferType, convert(new DateOnly(2024, 1, 2))));
    }

    [Fact]
    public void A_Timestamp_At_Midnight_Is_Stored_As_It_Comes()
    {
        var (bufferType, convert) = MySqlDataWriter.BufferColumn("date", typeof(DateTime), typeof(DateTime), "n");

        Assert.Equal(new DateTime(2024, 1, 2), Store(bufferType, convert(new DateTime(2024, 1, 2))));
    }

    [Fact]
    public void A_Time_Of_Day_Into_A_Date_Column_Is_Refused_Naming_Column_And_Value()
    {
        var (_, convert) = MySqlDataWriter.BufferColumn("date", typeof(DateTime), typeof(DateTime), "n");

        var ex = Assert.Throws<InvalidOperationException>(() => convert(new DateTime(2024, 1, 2, 13, 45, 0)));

        Assert.Contains("'n'", ex.Message);
        Assert.Contains("2024-01-02 13:45:00", ex.Message);
        Assert.Contains("CAST(n AS DATE)", ex.Message);

        var (_, intoDateTime) = MySqlDataWriter.BufferColumn("datetime", typeof(DateTime), typeof(DateTime), "n");
        Assert.Equal(new DateTime(2024, 1, 2, 13, 45, 0), intoDateTime(new DateTime(2024, 1, 2, 13, 45, 0)));
    }
}
