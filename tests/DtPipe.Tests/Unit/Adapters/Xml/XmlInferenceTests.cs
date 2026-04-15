using DtPipe.Adapters.Xml;
using DtPipe.Core.Models;
using FluentAssertions;
using Xunit;
using System.IO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DtPipe.Tests.Unit.Adapters.Xml;

public class XmlInferenceTests : IAsyncLifetime
{
    private string _testXmlPath = null!;

    public ValueTask InitializeAsync()
    {
        _testXmlPath = Path.Combine(Path.GetTempPath(), $"test_inf_{Guid.NewGuid()}.xml");
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (File.Exists(_testXmlPath)) File.Delete(_testXmlPath);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task XmlStreamReader_ShouldParseExplicitColumnTypesWithDotNotation()
    {
        // Arrange
        var content = """
            <Root>
              <Item>
                <Id>101</Id>
                <Meta>
                   <Price>19.99</Price>
                   <IsActive>true</IsActive>
                </Meta>
              </Item>
            </Root>
            """;
        await File.WriteAllTextAsync(_testXmlPath, content);

        var options = new XmlReaderOptions 
        { 
            Path = "//Item", 
            ColumnTypes = "Id:int32,Meta.Price:double,Meta.IsActive:bool" 
        };
        var reader = new XmlStreamReader(_testXmlPath, options);

        // Act
        await reader.OpenAsync();
        var batches = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(10))
        {
            batches.AddRange(batch.ToArray());
        }
        await reader.DisposeAsync();

        // Assert
        var cols = reader.Columns!.ToList();
        cols.First(c => c.Name == "Id").ClrType.Should().Be(typeof(int));
        cols.First(c => c.Name == "Meta.Price").ClrType.Should().Be(typeof(double));
        cols.First(c => c.Name == "Meta.IsActive").ClrType.Should().Be(typeof(bool));

        batches.Should().HaveCount(1);
        var row = batches[0];
        
        row[cols.FindIndex(c => c.Name == "Id")].Should().Be(101);
        row[cols.FindIndex(c => c.Name == "Meta.Price")].Should().Be(19.99);
        row[cols.FindIndex(c => c.Name == "Meta.IsActive")].Should().Be(true);
    }

    [Fact]
    public async Task XmlStreamReader_ShouldAutoInferSparseSchema()
    {
        // Arrange
        var content = """
            <Root>
              <Item>
                <Id>101</Id>
                <Tag>A</Tag>
              </Item>
              <Item>
                <Id>102</Id>
                <OptionalCol>FoundMe</OptionalCol>
              </Item>
            </Root>
            """;
        await File.WriteAllTextAsync(_testXmlPath, content);

        var options = new XmlReaderOptions 
        { 
            Path = "//Item", 
            AutoColumnTypes = true 
        };
        var reader = new XmlStreamReader(_testXmlPath, options);

        // Act
        await reader.OpenAsync();
        var batches = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(10))
        {
            batches.AddRange(batch.ToArray());
        }
        await reader.DisposeAsync();

        // Assert
        var colNames = reader.Columns!.Select(c => c.Name).ToList();
        
        // Both records contribute to the schema
        colNames.Should().Contain(new[] { "Id", "Tag", "OptionalCol" });
        
        // "Id" should be inferred as int32 because "101" and "102" were seen
        reader.Columns!.First(c => c.Name == "Id").ClrType.Should().Be(typeof(int));
    }
}
