using System.Globalization;
using AwesomeAssertions;
using DtPipe.Processors.DuckDB;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

/// <summary>
/// The column identifiers the table function's Init accepts from DuckDB's projection. DuckDB gives this function
/// none past its columns, reserved or not, so the refusal is judged here, off the function Init calls; a scan
/// that reads no column stays green under DuckDB (<c>DuckDBSqlProcessorTests</c>, <c>ScanReadingNoColumn</c>).
/// </summary>
public class DuckDbArrowTableFunctionTests
{
    private const int ColumnCount = 3;

    [Theory]
    [InlineData(0UL, false)]
    [InlineData(2UL, false)]
    [InlineData(3UL, false)]
    [InlineData(ulong.MaxValue - 2, false)]
    [InlineData(ulong.MaxValue - 1, true)] // COLUMN_IDENTIFIER_EMPTY
    [InlineData(ulong.MaxValue, true)] // COLUMN_IDENTIFIER_ROW_ID
    public void Only_The_Row_Id_And_The_Empty_Column_Are_Reserved(ulong columnId, bool reserved)
        => DuckDbArrowTableSource.IsReservedColumnId(columnId).Should().Be(reserved);

    [Theory]
    [InlineData(0UL, false)]
    [InlineData(2UL, false)]
    [InlineData(3UL, true)]
    [InlineData(ulong.MaxValue - 2, true)]
    [InlineData(ulong.MaxValue - 1, false)]
    [InlineData(ulong.MaxValue, false)]
    public void A_Column_Id_Past_The_Source_Is_Refused_Naming_It_Unless_Reserved(ulong columnId, bool refused)
    {
        var error = DuckDbArrowTableSource.UnknownColumnError("main", [columnId], ColumnCount);

        if (refused)
            error.Should().Contain("'main'").And.Contain(columnId.ToString(CultureInfo.InvariantCulture));
        else
            error.Should().BeNull();
    }

    [Fact]
    public void A_Projection_Is_Refused_For_Its_Unknown_Column_Among_Valid_And_Reserved_Ones()
    {
        var error = DuckDbArrowTableSource.UnknownColumnError("orders", [2, ulong.MaxValue, 0, 7], ColumnCount);

        error.Should().Contain("'orders'").And.Contain("column 7");
    }

    [Fact]
    public void A_Projection_Of_Columns_And_Reserved_Ids_Is_Accepted()
        => DuckDbArrowTableSource.UnknownColumnError("orders", [2, 0, 1, ulong.MaxValue - 1], ColumnCount).Should().BeNull();
}
