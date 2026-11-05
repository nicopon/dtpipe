using System;
using System.Data;
using Apache.Arrow.Ado;
using Apache.Arrow.Serialization;
using Apache.Arrow.Serialization.Mapping;
using Apache.Arrow.Types;
using Xunit;

namespace DtPipe.Tests.Unit.ApacheArrowAdo;

/// <summary>
/// A value the Arrow type cannot hold, or the driver cannot hand over, surfaces as one fault that
/// carries the column and the record, never the value.
/// </summary>
public class DecimalConsumerTests
{
    [Fact]
    public void A_Value_Past_The_Declared_Scale_Names_Its_Column_And_Record()
    {
        var fault = Assert.Throws<ArrowValueException>(() => Read(typeof(decimal), 4.29m, 1e-25m));

        Assert.Equal("v", fault.Column);
        Assert.Equal(2, fault.Record);
        Assert.Equal(nameof(OverflowException), fault.CauseTypeName);
        Assert.Null(fault.InnerException);
    }

    [Fact]
    public void A_Driver_That_Cannot_Hand_Over_A_Decimal_Names_Its_Column()
    {
        var fault = Assert.Throws<ArrowValueException>(() => Read(typeof(string), "not a number"));

        Assert.Equal("v", fault.Column);
        Assert.Equal(1, fault.Record);
        Assert.Equal(nameof(InvalidCastException), fault.CauseTypeName);
    }

    private static void Read(Type columnType, params object[] values)
    {
        using var table = new DataTable();
        table.Columns.Add("id", typeof(int));
        table.Columns.Add("v", columnType);
        foreach (var value in values) table.Rows.Add(table.Rows.Count + 1, value);

        var config = new AdoToArrowConfigBuilder()
            .SetTypeResolver(column => column.ColumnName == "v"
                ? new ArrowTypeResult(new Decimal128Type(38, 18))
                : new ArrowTypeResult(Int32Type.Default))
            .Build();

        using var reader = table.CreateDataReader();
        foreach (var batch in AdoToArrow.ReadToArrowBatches(reader, config)) batch.Dispose();
    }
}
