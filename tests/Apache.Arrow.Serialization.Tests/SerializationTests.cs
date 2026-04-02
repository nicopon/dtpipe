using Apache.Arrow;
using Apache.Arrow.Serialization;
using FluentAssertions;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Serialization.Tests;

public class SerializationTests
{
    public class SimplePoco
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
        public bool IsActive { get; set; }
    }

    [Fact]
    public async Task Serialize_SimplePoco_ShouldWork()
    {
        // Arrange
        var data = new List<SimplePoco>
        {
            new SimplePoco { Id = 1, Name = "A", Value = 1.1, IsActive = true },
            new SimplePoco { Id = 2, Name = "B", Value = 2.2, IsActive = false },
            new SimplePoco { Id = 3, Name = "C", Value = 3.3, IsActive = true }
        };

        // Act
        var recordBatch = await ArrowSerializer.SerializeAsync(data);

        // Assert
        recordBatch.Length.Should().Be(data.Count);
        recordBatch.ColumnCount.Should().Be(4);
        
        var idColumn = (Int32Array)recordBatch.Column(0);
        idColumn.GetValue(0).Should().Be(1);
        idColumn.GetValue(1).Should().Be(2);

        var isActiveColumn = (BooleanArray)recordBatch.Column(1);
        isActiveColumn.GetValue(0).Should().Be(true);
        isActiveColumn.GetValue(1).Should().Be(false);

        var nameColumn = (StringArray)recordBatch.Column(2);
        nameColumn.GetString(0).Should().Be("A");
        nameColumn.GetString(1).Should().Be("B");

        var valueColumn = (DoubleArray)recordBatch.Column(3);
        valueColumn.GetValue(0).Should().Be(1.1);
        valueColumn.GetValue(1).Should().Be(2.2);
    }

    public class ComplexPoco
    {
        public Guid UniqueId { get; set; }
        public int? OptionalInt { get; set; }
        public string? OptionalString { get; set; }
        public MyEnum Category { get; set; }
    }

    public enum MyEnum { First, Second, Third }

    [Fact]
    public async Task SerializeDeserialize_ComplexPoco_ShouldBeSymmetric()
    {
        // Arrange
        var data = new List<ComplexPoco>
        {
            new ComplexPoco { UniqueId = Guid.NewGuid(), OptionalInt = 42, OptionalString = "Hello", Category = MyEnum.Second },
            new ComplexPoco { UniqueId = Guid.NewGuid(), OptionalInt = null, OptionalString = null, Category = MyEnum.First }
        };

        // Act
        var recordBatch = await ArrowSerializer.SerializeAsync(data);
        var result = await ArrowDeserializer.DeserializeAsync<ComplexPoco>(recordBatch).ToListAsync();

        // Assert
        result.Should().HaveCount(data.Count);
        result[0].UniqueId.Should().Be(data[0].UniqueId);
        result[0].OptionalInt.Should().Be(42);
        result[0].OptionalString.Should().Be("Hello");
        result[0].Category.Should().Be(MyEnum.Second);

        result[1].UniqueId.Should().Be(data[1].UniqueId);
        result[1].OptionalInt.Should().BeNull();
        result[1].OptionalString.Should().BeNull();
        result[1].Category.Should().Be(MyEnum.First);
    }

    public class NestedPoco
    {
        public string Title { get; set; }
        public SimplePoco Inner { get; set; }
    }

    [Fact]
    public async Task SerializeDeserialize_NestedPoco_ShouldWork()
    {
        // Arrange
        var data = new List<NestedPoco>
        {
            new NestedPoco 
            { 
                Title = "Outer 1", 
                Inner = new SimplePoco { Id = 1, Name = "Inner 1", IsActive = true } 
            },
            new NestedPoco 
            { 
                Title = "Outer 2", 
                Inner = null // Test null struct
            }
        };

        // Act
        var recordBatch = await ArrowSerializer.SerializeAsync(data);
        var result = await ArrowDeserializer.DeserializeAsync<NestedPoco>(recordBatch).ToListAsync();

        // Assert
        result.Should().HaveCount(2);
        result[0].Title.Should().Be("Outer 1");
        result[0].Inner.Should().NotBeNull();
        result[0].Inner.Id.Should().Be(1);
        result[0].Inner.Name.Should().Be("Inner 1");

        result[1].Title.Should().Be("Outer 2");
        result[1].Inner.Should().BeNull();
    }

    public class ListPoco
    {
        public string Name { get; set; }
        public List<int> Scores { get; set; }
        public string[] Tags { get; set; }
    }

    [Fact]
    public async Task SerializeDeserialize_ListPoco_ShouldWork()
    {
        // Arrange
        var data = new List<ListPoco>
        {
            new ListPoco 
            { 
                Name = "John", 
                Scores = new List<int> { 10, 20, 30 }, 
                Tags = new[] { "tag1", "tag2" } 
            },
            new ListPoco 
            { 
                Name = "Jane", 
                Scores = new List<int>(), 
                Tags = null 
            }
        };

        // Act
        var recordBatch = await ArrowSerializer.SerializeAsync(data);
        var result = await ArrowDeserializer.DeserializeAsync<ListPoco>(recordBatch).ToListAsync();

        // Assert
        result.Should().HaveCount(2);
        
        result[0].Name.Should().Be("John");
        result[0].Scores.Should().Equal(10, 20, 30);
        result[0].Tags.Should().Equal("tag1", "tag2");

        result[1].Name.Should().Be("Jane");
        result[1].Scores.Should().BeEmpty();
        result[1].Tags.Should().BeNull();
    }

    [Fact]
    public async Task SerializeDeserialize_IoTTelemetry_ShouldBeSymmetric()
    {
        // Arrange
        var data = new List<IoTTelemetry>
        {
            new IoTTelemetry
            {
                DeviceId = Guid.NewGuid(),
                DeviceName = "WeatherStation-01",
                Timestamp = new DateTime(2026, 4, 2, 10, 0, 0, DateTimeKind.Utc),
                Readings = new[] { 22.5, 60.0, 1013.2 },
                Metadata = new Dictionary<string, string> { { "Location", "Paris" }, { "Model", "TX-100" } },
                SensorInfo = new SensorMetadata { SerialNumber = "SN123", FirmwareVersion = "v1.2", CalibrationYear = 2025 }
            },
            new IoTTelemetry
            {
                DeviceId = Guid.NewGuid(),
                DeviceName = "MotionSensor-02",
                Timestamp = new DateTime(2026, 4, 2, 11, 0, 0, DateTimeKind.Utc),
                Readings = System.Array.Empty<double>(),
                Metadata = new Dictionary<string, string>(),
                SensorInfo = new SensorMetadata { SerialNumber = "SN999", FirmwareVersion = "v0.9", CalibrationYear = 2024 }
            }
        };

        // Act
        var recordBatch = await ArrowSerializer.SerializeAsync(data);
        var result = await ArrowDeserializer.DeserializeAsync<IoTTelemetry>(recordBatch).ToListAsync();

        // Assert
        result.Should().BeEquivalentTo(data, options => options
             .Using<DateTime>(ctx => ctx.Subject.Should().BeCloseTo(ctx.Expectation, TimeSpan.FromMilliseconds(1)))
             .WhenTypeIs<DateTime>());
    }

    [Fact]
    public async Task SerializeDeserialize_ECommerceOrder_ShouldBeSymmetric()
    {
        // Arrange
        var data = new List<ECommerceOrder>
        {
            new ECommerceOrder
            {
                OrderId = 1001,
                Status = OrderStatus.Processing,
                TotalAmount = 149.99m,
                Currency = Currency.EUR,
                OrderedAt = DateTimeOffset.UtcNow,
                Items = new List<OrderItem>
                {
                    new OrderItem { ProductSku = "P-001", Quantity = 1, UnitPrice = 99.99m },
                    new OrderItem { ProductSku = "P-002", Quantity = 2, UnitPrice = 25.00m }
                }
            }
        };

        // Act
        var recordBatch = await ArrowSerializer.SerializeAsync(data);
        var result = await ArrowDeserializer.DeserializeAsync<ECommerceOrder>(recordBatch).ToListAsync();

        // Assert
        result.Should().BeEquivalentTo(data, options => options
            .Using<DateTimeOffset>(ctx => ctx.Subject.Should().BeCloseTo(ctx.Expectation, TimeSpan.FromMilliseconds(1)))
            .WhenTypeIs<DateTimeOffset>());
    }

    [Fact]
    public async Task SerializeDeserialize_UserAccount_ShouldBeSymmetric()
    {
        // Arrange
        var data = new List<UserAccount>
        {
            new UserAccount
            {
                AccountId = Guid.NewGuid(),
                Email = "user@example.com",
                IsActive = true,
                Roles = new HashSet<string> { "Admin", "User" },
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-10),
                SessionTimeout = TimeSpan.FromMinutes(30)
            }
        };

        // Act
        var recordBatch = await ArrowSerializer.SerializeAsync(data);
        var result = await ArrowDeserializer.DeserializeAsync<UserAccount>(recordBatch).ToListAsync();

        // Assert
        result.Should().BeEquivalentTo(data, options => options
            .Using<DateTimeOffset>(ctx => ctx.Subject.Should().BeCloseTo(ctx.Expectation, TimeSpan.FromMilliseconds(1)))
            .WhenTypeIs<DateTimeOffset>());
    }

    [Fact]
    public async Task Serialize_ExpandoObject_ShouldWork()
    {
        // Arrange
        dynamic obj1 = new System.Dynamic.ExpandoObject();
        obj1.Id = 1;
        obj1.Name = "Test1";

        dynamic obj2 = new System.Dynamic.ExpandoObject();
        obj2.Id = 2;
        obj2.Name = "Test2";

        var data = new List<object> { obj1, obj2 };

        // Act
        var batch = await ArrowSerializer.SerializeAsync(data);

        // Assert
        batch.Length.Should().Be(2);
        batch.ColumnCount.Should().Be(2);
        batch.Schema.FieldsList[0].Name.Should().Be("Id");
        batch.Schema.FieldsList[1].Name.Should().Be("Name");

        var idCol = (Int32Array)batch.Column(0);
        idCol.GetValue(0).Should().Be(1);
        idCol.GetValue(1).Should().Be(2);
    }

    [Fact]
    public async Task Serialize_JsonObject_ShouldWork()
    {
        // Arrange
        var jo = new System.Text.Json.Nodes.JsonObject
        {
            ["Id"] = 10,
            ["Price"] = 99.99
        };
        var data = new List<System.Text.Json.Nodes.JsonObject> { jo };

        // Act
        var batch = await ArrowSerializer.SerializeAsync(data);

        // Assert
        batch.Length.Should().Be(1);
        ((Int32Array)batch.Column(0)).GetValue(0).Should().Be(10);
        ((DoubleArray)batch.Column(1)).GetValue(0).Should().Be(99.99);
    }

    [Fact]
    public async Task Serialize_Dynamic_MissingKeys_ShouldAppendNulls()
    {
        // Arrange
        dynamic obj1 = new System.Dynamic.ExpandoObject();
        obj1.Id = 1;
        obj1.Value = 10.5;

        dynamic obj2 = new System.Dynamic.ExpandoObject();
        obj2.Id = 2;
        // Missing "Value"

        var data = new List<object> { obj1, obj2 };

        // Act
        var batch = await ArrowSerializer.SerializeAsync(data);

        // Assert
        var valCol = (DoubleArray)batch.Column(1);
        valCol.GetValue(0).Should().Be(10.5);
        valCol.IsNull(1).Should().BeTrue();
    }

    [Fact]
    public async Task Serialize_Dynamic_ExtraKeys_ShouldThrow()
    {
        // Arrange
        dynamic obj1 = new System.Dynamic.ExpandoObject();
        obj1.Id = 1;

        dynamic obj2 = new System.Dynamic.ExpandoObject();
        obj2.Id = 2;
        obj2.Extra = "Forbidden";

        var data = new List<object> { obj1, obj2 };

        // Act & Assert
        var act = async () => await ArrowSerializer.SerializeAsync(data);
        await act.Should().ThrowAsync<System.InvalidOperationException>()
            .WithMessage("*unknown key 'Extra'*");
    }
}
