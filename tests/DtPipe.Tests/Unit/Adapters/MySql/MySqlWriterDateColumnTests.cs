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
}
