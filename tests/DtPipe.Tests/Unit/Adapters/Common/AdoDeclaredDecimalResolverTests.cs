using System.Data.Common;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using DtPipe.Adapters.Common;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Common;

/// <summary>
/// The ADO readers share one type resolver, so a decimal the source declares a width for reaches
/// the Arrow batches at that width, and one that declares none keeps the default.
/// </summary>
public class AdoDeclaredDecimalResolverTests
{
    [Theory]
    [InlineData(18, 9, 18, 9)]
    [InlineData(10, 2, 10, 2)]
    [InlineData(38, 0, 38, 0)]
    [InlineData(38, 5, 38, 5)]
    public void A_Declared_Decimal_Keeps_Its_Width(int precision, int scale, int expectedPrecision, int expectedScale)
    {
        var type = Resolve(typeof(decimal), precision, scale);

        Assert.Equal((expectedPrecision, expectedScale), Shape(type));
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(null, 2)]
    [InlineData(10, null)]
    [InlineData(39, 2)]
    [InlineData(10, 11)]
    [InlineData(10, -1)]
    public void A_Decimal_Without_A_Usable_Declaration_Keeps_The_Default(int? precision, int? scale)
    {
        var type = Resolve(typeof(decimal), precision, scale);

        Assert.Equal(Shape(ArrowTypeMap.DefaultDecimalType), Shape(type));
    }

    [Fact]
    public void A_Column_Is_Matched_By_Ordinal_Not_By_Name()
    {
        var reader = new ProbeReader(
            new PipeColumnInfo("a", typeof(decimal), true, Precision: 10, Scale: 2),
            new PipeColumnInfo("a", typeof(decimal), true));

        Assert.Equal((10, 2), Shape(reader.Resolve(new TestColumn("a", typeof(decimal), 0))));
        Assert.Equal(Shape(ArrowTypeMap.DefaultDecimalType), Shape(reader.Resolve(new TestColumn("a", typeof(decimal), 1))));
    }

    [Fact]
    public void The_Declared_Clr_Type_Wins_Over_The_Driver_One()
    {
        // ODP.NET reports NUMBER(10,2) as Double; the reader declares it decimal.
        var reader = new ProbeReader(new PipeColumnInfo("n", typeof(decimal), true, Precision: 10, Scale: 2));

        Assert.Equal((10, 2), Shape(reader.Resolve(new TestColumn("n", typeof(double), 0))));
    }

    [Fact]
    public void A_Column_Of_Another_Type_Is_Unaffected_By_A_Declared_Width()
    {
        var type = Resolve(typeof(long), 10, 0);

        Assert.IsType<Int64Type>(type);
    }

    private static (int Precision, int Scale) Shape(IArrowType type)
        => type is Decimal128Type d ? (d.Precision, d.Scale) : throw new InvalidOperationException(type.Name);

    private static IArrowType Resolve(Type clr, int? precision, int? scale)
        => new ProbeReader(new PipeColumnInfo("c", clr, true, Precision: precision, Scale: scale))
            .Resolve(new TestColumn("c", clr, 0));

    private sealed class ProbeReader : AdoColumnarReader
    {
        public ProbeReader(params PipeColumnInfo[] columns) => Columns = columns;

        public override Task OpenAsync(CancellationToken ct = default) => Task.CompletedTask;

        public IArrowType Resolve(DbColumn column) => DeclaredTypeResolver(column).ArrowType;
    }

    private sealed class TestColumn : DbColumn
    {
        public TestColumn(string name, Type clr, int ordinal)
        {
            ColumnName = name;
            DataType = clr;
            ColumnOrdinal = ordinal;
        }
    }
}
