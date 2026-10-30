using System.Data;
using DtPipe.Adapters.SqlServer;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.SqlServer;

// The row path stores each cell in a DataTable column of the buffer type before SqlBulkCopy sends it,
// so a value is checked by storing it there, which is where a mismatched type fails. A date column's
// inferred type is the one SqlServerTypeConverter gives "date".
public class SqlServerWriterDateColumnTests
{
    private static readonly Type DateColumnType = SqlServerTypeConverter.Instance.MapFromProviderType("date");

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
        var (bufferType, convert) = SqlServerDataWriter.BufferColumn("date", DateColumnType, typeof(DateOnly), "n");

        Assert.Equal(new DateOnly(2024, 1, 2), Store(bufferType, convert(new DateOnly(2024, 1, 2))));
    }

    [Fact]
    public void A_Text_Or_Timestamp_Source_Is_Stored_As_A_Timestamp_At_Midnight()
    {
        var (textBuffer, fromText) = SqlServerDataWriter.BufferColumn("date", DateColumnType, typeof(string), "n");
        var (timestampBuffer, fromTimestamp) = SqlServerDataWriter.BufferColumn("date", DateColumnType, typeof(DateTime), "n");

        Assert.Equal(new DateTime(2024, 1, 2), Store(textBuffer, fromText("2024-01-02")));
        Assert.Equal(new DateTime(2024, 1, 2), Store(timestampBuffer, fromTimestamp(new DateTime(2024, 1, 2))));
    }

    [Fact]
    public void A_Time_Of_Day_Into_A_Date_Column_Is_Refused_Naming_Column_And_Value()
    {
        var (_, convert) = SqlServerDataWriter.BufferColumn("date", DateColumnType, typeof(DateTime), "n");

        var ex = Assert.Throws<InvalidOperationException>(() => convert(new DateTime(2024, 1, 2, 13, 45, 0)));

        Assert.Contains("'n'", ex.Message);
        Assert.Contains("2024-01-02 13:45:00", ex.Message);
        Assert.Contains("CAST(n AS DATE)", ex.Message);
        Assert.Contains("datetime2", ex.Message);

        var (_, intoDateTime2) = SqlServerDataWriter.BufferColumn("datetime2", typeof(DateTime), typeof(DateTime), "n");
        Assert.Equal(new DateTime(2024, 1, 2, 13, 45, 0), intoDateTime2(new DateTime(2024, 1, 2, 13, 45, 0)));
    }
}
