using System;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Ado.Consumer;
using Apache.Arrow.Types;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.ApacheArrowAdo;

/// <summary>
/// A decimal the Arrow type cannot hold, or the driver cannot hand over as a <see cref="decimal"/>,
/// surfaces as one exception that carries the column, never the value.
/// </summary>
public class DecimalConsumerTests
{
    [Fact]
    public void A_Value_That_Fits_Is_Appended()
    {
        var consumer = new Decimal128Consumer(2, new Decimal128Type(38, 18));

        consumer.Consume(ReaderOf(2, 4.29m));

        var array = (Decimal128Array)consumer.BuildArray();
        Assert.Equal(4.29m, array.GetValue(0));
    }

    [Fact]
    public void A_Value_Past_The_Declared_Scale_Names_Its_Column()
    {
        var consumer = new Decimal128Consumer(2, new Decimal128Type(38, 18));

        var fault = Assert.Throws<DecimalOutOfRangeException>(() => consumer.Consume(ReaderOf(2, 1e-25m)));

        Assert.Equal(2, fault.ColumnIndex);
        Assert.Equal(nameof(OverflowException), fault.CauseTypeName);
        Assert.Null(fault.InnerException);
    }

    [Theory]
    [InlineData(typeof(OverflowException))]
    [InlineData(typeof(InvalidCastException))]
    public void A_Driver_That_Cannot_Hand_Over_A_Decimal_Names_Its_Column(Type driverFault)
    {
        var reader = new Mock<DbDataReader>();
        reader.Setup(r => r.IsDBNull(1)).Returns(false);
        reader.Setup(r => r.GetDecimal(1)).Throws((Exception)Activator.CreateInstance(driverFault)!);
        var consumer = new Decimal256Consumer(1, new Decimal256Type(76, 10));

        var fault = Assert.Throws<DecimalOutOfRangeException>(() => consumer.Consume(reader.Object));

        Assert.Equal(1, fault.ColumnIndex);
        Assert.Equal(driverFault.Name, fault.CauseTypeName);
    }

    private static DbDataReader ReaderOf(int ordinal, decimal value)
    {
        var reader = new Mock<DbDataReader>();
        reader.Setup(r => r.IsDBNull(ordinal)).Returns(false);
        reader.Setup(r => r.GetDecimal(ordinal)).Returns(value);
        return reader.Object;
    }
}
