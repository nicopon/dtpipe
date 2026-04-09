using System;
using System.Data;
using System.Data.Common;
using Apache.Arrow;
using Apache.Arrow.Ado.Consumer;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.ApacheArrowAdo;

public class BinaryConsumerTests
{
    [Fact]
    public void BinaryConsumer_AppendsBytesCorrectly()
    {
        // Arrange
        var mockReader = new Mock<DbDataReader>();
        var bytes = new byte[] { 1, 2, 3, 4 };
        
        mockReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockReader.Setup(r => r.GetValue(0)).Returns(bytes);
        
        var consumer = new BinaryConsumer(0);
        
        // Act
        consumer.Consume(mockReader.Object);
        var array = (BinaryArray)consumer.BuildArray();
        
        // Assert
        Assert.Equal(1, array.Length);
        Assert.Equal(bytes, array.GetBytes(0).ToArray());
    }

    [Fact]
    public void BinaryConsumer_AppendsNullCorrectly()
    {
        // Arrange
        var mockReader = new Mock<DbDataReader>();
        mockReader.Setup(r => r.IsDBNull(0)).Returns(true);
        
        var consumer = new BinaryConsumer(0);
        
        // Act
        consumer.Consume(mockReader.Object);
        var array = (BinaryArray)consumer.BuildArray();
        
        // Assert
        Assert.Equal(1, array.Length);
        Assert.True(array.IsNull(0));
    }
}
