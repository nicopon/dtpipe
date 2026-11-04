using System;
using System.Data.Common;
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Ado.Consumer;
using Apache.Arrow.Types;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.ApacheArrowAdo;

public class TimeConsumerTests
{
    private static Mock<DbDataReader> ReaderOf(TimeSpan value)
    {
        var reader = new Mock<DbDataReader>();
        reader.Setup(r => r.IsDBNull(0)).Returns(false);
        reader.Setup(r => r.GetValue(0)).Returns(value);
        reader.Setup(r => r.GetName(0)).Returns("opens_at");
        return reader;
    }

    [Fact]
    public void ATimeOfDay_KeepsItsMicroseconds()
    {
        var consumer = new Time64Consumer(0);

        consumer.Consume(ReaderOf(new TimeSpan(23, 59, 59) + TimeSpan.FromTicks(1_234_560)).Object);
        var array = (Time64Array)consumer.BuildArray();

        Assert.Equal(TimeUnit.Microsecond, ((Time64Type)array.Data.DataType).Unit);
        Assert.Equal(86_399_123_456L, array.GetValue(0));
    }

    [Theory]
    [InlineData(24, 0, 0)]
    [InlineData(838, 59, 59)]
    public void AValueBeyondOneDay_IsRefusedByColumnName(int hours, int minutes, int seconds)
    {
        var consumer = new Time64Consumer(0);

        var error = Assert.Throws<InvalidDataException>(
            () => consumer.Consume(ReaderOf(new TimeSpan(hours, minutes, seconds)).Object));

        Assert.Contains("'opens_at'", error.Message);
    }

    [Fact]
    public void ANegativeValue_IsRefusedByColumnName()
    {
        var consumer = new Time64Consumer(0);

        var error = Assert.Throws<InvalidDataException>(() => consumer.Consume(ReaderOf(TimeSpan.FromHours(-1)).Object));

        Assert.Contains("'opens_at'", error.Message);
    }

    [Fact]
    public void ASqlTimeColumn_ResolvesToTime64Microseconds()
    {
        var type = AdoToArrowUtils.GetLogicalTypeFromDbColumn(new TimeColumn()).ArrowType;

        Assert.Equal(TimeUnit.Microsecond, Assert.IsType<Time64Type>(type).Unit);
        Assert.IsType<Time64Consumer>(AdoConsumerFactory.Create(type, 0));
    }

    private sealed class TimeColumn : DbColumn
    {
        public TimeColumn()
        {
            DataType = typeof(TimeSpan);
            DataTypeName = "time";
        }
    }
}
