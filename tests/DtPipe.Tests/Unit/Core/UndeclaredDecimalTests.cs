using Apache.Arrow;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// An Arrow decimal always carries a precision and scale, so a width the source never declared
/// would read back as a declared (38,18) once a schema is turned into columns again. The default
/// is marked, and a column that goes through the schema keeps what the source said.
/// </summary>
public class UndeclaredDecimalTests
{
    [Fact]
    public void A_Decimal_With_No_Declared_Width_Is_Marked()
    {
        var field = ArrowSchemaFactory.Create(new[] { new PipeColumnInfo("d", typeof(decimal), true) }).FieldsList[0];

        Assert.True(ArrowTypeMap.IsUndeclaredDecimal(field));
    }

    [Fact]
    public void A_Declared_Decimal_Is_Not_Marked_Even_At_The_Default_Width()
    {
        var fields = ArrowSchemaFactory.Create(new[]
        {
            new PipeColumnInfo("a", typeof(decimal), true, Precision: 38, Scale: 18),
            new PipeColumnInfo("b", typeof(decimal), true, Precision: 10, Scale: 2),
        }).FieldsList;

        Assert.All(fields, f => Assert.False(ArrowTypeMap.IsUndeclaredDecimal(f)));
    }

    [Fact]
    public void A_Declaration_The_Engine_Cannot_Express_Falls_Back_To_The_Marked_Default()
    {
        var field = ArrowSchemaFactory.Create(new[] { new PipeColumnInfo("d", typeof(decimal), true, Precision: 60, Scale: 2) }).FieldsList[0];

        Assert.True(ArrowTypeMap.IsUndeclaredDecimal(field));
    }

    [Fact]
    public void A_Column_Keeps_What_The_Source_Declared_Through_The_Schema()
    {
        var columns = new[]
        {
            new PipeColumnInfo("declared", typeof(decimal), true, Precision: 38, Scale: 18),
            new PipeColumnInfo("narrow", typeof(decimal), true, Precision: 10, Scale: 2),
            new PipeColumnInfo("none", typeof(decimal), true),
        };

        var back = ArrowSchemaFactory.ToPipeColumns(ArrowSchemaFactory.Create(columns));

        Assert.Equal((38, 18), (back[0].Precision, back[0].Scale));
        Assert.Equal((10, 2), (back[1].Precision, back[1].Scale));
        Assert.Null(back[2].Precision);
        Assert.Null(back[2].Scale);
    }

    [Fact]
    public void The_Mark_Survives_A_Schema_File_And_Stays_Off_A_Declared_Decimal()
    {
        var schema = ArrowSchemaFactory.Create(new[]
        {
            new PipeColumnInfo("none", typeof(decimal), true),
            new PipeColumnInfo("declared", typeof(decimal), true, Precision: 38, Scale: 18),
        });

        var json = ArrowSchemaSerializer.SerializeCompact(schema);
        var read = ArrowSchemaSerializer.Deserialize(json);

        Assert.True(ArrowTypeMap.IsUndeclaredDecimal(read.FieldsList[0]));
        Assert.False(ArrowTypeMap.IsUndeclaredDecimal(read.FieldsList[1]));
    }

    [Fact]
    public void A_Contract_Written_Before_The_Mark_Reads_As_Declared()
    {
        // No metadata on a decimal: every schema written before the mark existed.
        var read = ArrowSchemaSerializer.Deserialize(
            "{\"fields\":[{\"name\":\"d\",\"nullable\":true,\"type\":\"decimal128:38:18\"}]}");

        var column = ArrowSchemaFactory.ToPipeColumns(read)[0];

        Assert.Equal((38, 18), (column.Precision, column.Scale));
    }

    [Fact]
    public void A_Column_Added_After_The_Source_Keeps_Its_Declared_Or_Undeclared_Width()
    {
        var source = new Schema(new[] { new Field("id", Int32Type.Default, true) }, null);
        var evolved = DtPipe.ExportService.EvolveSchema(source, new[]
        {
            new PipeColumnInfo("id", typeof(int), true),
            new PipeColumnInfo("none", typeof(decimal), true),
            new PipeColumnInfo("narrow", typeof(decimal), true, Precision: 10, Scale: 2),
        });

        Assert.True(ArrowTypeMap.IsUndeclaredDecimal(evolved.FieldsList[1]));
        Assert.False(ArrowTypeMap.IsUndeclaredDecimal(evolved.FieldsList[2]));
        Assert.Equal((10, 2), (((Decimal128Type)evolved.FieldsList[2].DataType).Precision, ((Decimal128Type)evolved.FieldsList[2].DataType).Scale));
    }

    [Fact]
    public void A_Non_Decimal_Is_Never_Undeclared()
        => Assert.False(ArrowTypeMap.IsUndeclaredDecimal(
            new Field("n", Int32Type.Default, true, new Dictionary<string, string> { [ArrowTypeMap.DecimalWidthKey] = ArrowTypeMap.UndeclaredDecimalWidth })));
}
