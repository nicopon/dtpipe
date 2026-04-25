using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class MemoryChannelRegistryTests
{
    [Fact]
    public void UpdateChannelColumns_ShouldNotDowngradeRichArrowSchema()
    {
        // Arrange
        var registry = new MemoryChannelRegistry();
        var alias = "orders";
        
        // 1. Register a rich Arrow schema (with a Struct)
        var richSchema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Field(f => f.Name("user").DataType(new StructType(new List<Field> { 
                new Field("name", StringType.Default, true) 
            })))
            .Build();
            
        registry.RegisterArrowChannel(alias, System.Threading.Channels.Channel.CreateUnbounded<RecordBatch>(), richSchema);
        
        // 2. Perform a row-based update with simplified types (e.g. from a transformer that doesn't know about Structs)
        var rowColumns = new List<PipeColumnInfo>
        {
            new("id", typeof(int), true),
            new("user", typeof(string), true) // Downgraded to string
        };
        
        // Act
        registry.UpdateChannelColumns(alias, rowColumns);
        
        // Assert
        var schema = registry.GetArrowChannel(alias)!.Value.Schema;
        
        // The StructType should be preserved
        Assert.IsType<StructType>(schema.FieldsList[1].DataType);
        Assert.Equal("id", schema.FieldsList[0].Name);
        Assert.Equal("user", schema.FieldsList[1].Name);
    }

    [Fact]
    public void UpdateChannelColumns_ShouldOverwriteSimpleArrowSchema()
    {
        // Arrange
        var registry = new MemoryChannelRegistry();
        var alias = "simple";
        
        // 1. Register a simple Arrow schema (no structs/lists)
        var simpleSchema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int32Type.Default))
            .Build();
            
        registry.RegisterArrowChannel(alias, System.Threading.Channels.Channel.CreateUnbounded<RecordBatch>(), simpleSchema);
        
        // 2. Perform a row-based update with new columns
        var rowColumns = new List<PipeColumnInfo>
        {
            new("id", typeof(int), true),
            new("name", typeof(string), true)
        };
        
        // Act
        registry.UpdateChannelColumns(alias, rowColumns);
        
        // Assert
        var schema = registry.GetArrowChannel(alias)!.Value.Schema;
        
        // Should have 2 columns now
        Assert.Equal(2, schema.FieldsList.Count);
        Assert.Equal("name", schema.FieldsList[1].Name);
    }
}
