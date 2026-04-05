using System;
using Apache.Arrow;
using Apache.Arrow.Serialization;
using Apache.Arrow.Types;
using FluentAssertions;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Serialization.Tests;

public class DynamicSerializationTests
{
    [Fact]
    public async Task Serialize_DynamicNestedDictionary_ShouldCreateStructs()
    {
        // Arrange - similar to complex_data.jsonl
        var data = new List<Dictionary<string, object?>>
        {
            new() { 
                ["id"] = 1, 
                ["user"] = new Dictionary<string, object?> { ["name"] = "Alice", ["points"] = 100.0 },
                ["items"] = new List<object?> { 
                    new Dictionary<string, object?> { ["id"] = "A1", ["price"] = 10.5 },
                    new Dictionary<string, object?> { ["id"] = "A2", ["price"] = 5.0 }
                }
            },
            new() { 
                ["id"] = 2, 
                ["user"] = new Dictionary<string, object?> { ["name"] = "Bob", ["points"] = 200.0 },
                ["items"] = new List<object?> { 
                    new Dictionary<string, object?> { ["id"] = "B1", ["price"] = 20.0 }
                }
            }
        };

        // Act
        RecordBatch recordBatch;
        try
        {
            recordBatch = await ArrowSerializer.SerializeAsync(data);
        }
        catch (Exception ex)
        {
            throw new Exception($"Serialization failed: {ex.Message}\n{ex.StackTrace}", ex);
        }

        // Assert
        recordBatch.Length.Should().Be(2);
        recordBatch.ColumnCount.Should().Be(3);
        recordBatch.Schema.FieldsList[0].Name.Should().Be("id");
        recordBatch.Schema.FieldsList[1].Name.Should().Be("items");
        recordBatch.Schema.FieldsList[2].Name.Should().Be("user");

        // Verify nested 'user' struct
        recordBatch.Schema.FieldsList[2].DataType.Should().BeOfType<StructType>();
        var userStructType = (StructType)recordBatch.Schema.FieldsList[2].DataType;
        var userStruct = (StructArray)recordBatch.Column(2);

        int nameIdx = userStructType.GetFieldIndex("name");
        int pointsIdx = userStructType.GetFieldIndex("points");

        var nameField = (StringArray)userStruct.Fields[nameIdx];
        var pointsField = (DoubleArray)userStruct.Fields[pointsIdx];
        
        nameField.GetString(0).Should().Be("Alice");
        pointsField.GetValue(0).Should().Be(100.0);

        // Verify nested 'items' list
        recordBatch.Schema.FieldsList[1].DataType.Should().BeOfType<ListType>();
        var itemsList = (ListArray)recordBatch.Column(1);
        itemsList.Length.Should().Be(2);
    }

    [Fact]
    public async Task Serialize_WithExplicitSchema_ShouldEnforceStructTypes()
    {
        // Arrange - explicit schema with nested struct
        var schema = new Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("user", new StructType(new List<Field> {
                new Field("name", StringType.Default, true),
                new Field("points", DoubleType.Default, true)
            }), true))
            .Build();

        var data = new List<Dictionary<string, object?>>
        {
            // Row 1 - user is null but schema says it's a struct
            new() { ["id"] = 1, ["user"] = null },
            // Row 2 - user is partially present
            new() { ["id"] = 2, ["user"] = new Dictionary<string, object?> { ["name"] = "Alice" } }
        };

        // Act
        var recordBatch = await ArrowSerializer.SerializeAsync(data, schema);

        // Assert
        recordBatch.Length.Should().Be(2);
        recordBatch.Schema.FieldsList[1].DataType.Should().BeOfType<StructType>();
        
        var userStruct = (StructArray)recordBatch.Column(1);
        userStruct.IsNull(0).Should().BeTrue();
        userStruct.IsNull(1).Should().BeFalse();

        var nestedNameField = (StringArray)userStruct.Fields[0];
        nestedNameField.GetString(1).Should().Be("Alice");
    }
}
