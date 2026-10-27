using DtPipe.Adapters.Xml;
using DtPipe.Core.Models;
using AwesomeAssertions;
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
        cols.Should().Contain(c => c.Name == "Id");
        cols.Should().Contain(c => c.Name == "Meta");

        var idCol = cols.First(c => c.Name == "Id");
        idCol.ClrType.Should().Be(typeof(int));

        var metaCol = cols.First(c => c.Name == "Meta");
        metaCol.ClrType.Should().Be(typeof(object));

        batches.Should().HaveCount(1);
        var row = batches[0];

        row[cols.FindIndex(c => c.Name == "Id")].Should().Be(101);

        var metaDict = row[cols.FindIndex(c => c.Name == "Meta")] as Dictionary<string, object?>;
        metaDict.Should().NotBeNull();
        metaDict!["Price"].Should().Be(19.99);
        metaDict!["IsActive"].Should().Be(true);
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

    [Fact]
    public async Task XmlStreamReader_MaxSample_LimitsInferenceRecordCount()
    {
        var content = """
            <Root>
              <Item>
                <Id>101</Id>
                <Title>Item 1</Title>
              </Item>
              <Item>
                <Id>102</Id>
                <Title>Item 2</Title>
              </Item>
              <Item>
                <Id>103</Id>
                <Title>Item 3</Title>
                <LateDiscoveredCol>Surprise</LateDiscoveredCol>
              </Item>
            </Root>
            """;
        await File.WriteAllTextAsync(_testXmlPath, content);

        // MaxSample = 2: only the first 2 items are sampled, LateDiscoveredCol is not discovered
        var optionsLimited = new XmlReaderOptions { Path = "//Item", MaxSample = 2 };
        await using (var reader = new XmlStreamReader(_testXmlPath, optionsLimited))
        {
            await reader.OpenAsync();
            var colNames = reader.Columns!.Select(c => c.Name).ToList();
            colNames.Should().Contain(new[] { "Id", "Title" });
            colNames.Should().NotContain("LateDiscoveredCol");
        }

        // MaxSample = 3: the 3rd item is reached, LateDiscoveredCol is included in the schema
        var optionsBroader = new XmlReaderOptions { Path = "//Item", MaxSample = 3 };
        await using (var reader = new XmlStreamReader(_testXmlPath, optionsBroader))
        {
            await reader.OpenAsync();
            var colNames = reader.Columns!.Select(c => c.Name).ToList();
            colNames.Should().Contain(new[] { "Id", "Title", "LateDiscoveredCol" });
        }
    }

    [Fact]
    public async Task XmlStreamReader_MaxSample_Zero_WithoutDeclaredSchema_ThrowsInvalidOperationException()
    {
        var content = """
            <Root>
              <Item>
                <Id>101</Id>
                <Title>Item 1</Title>
              </Item>
            </Root>
            """;
        await File.WriteAllTextAsync(_testXmlPath, content);

        var optionsZero = new XmlReaderOptions { Path = "//Item", MaxSample = 0 };
        await using (var reader = new XmlStreamReader(_testXmlPath, optionsZero))
        {
            var open = async () => await reader.OpenAsync();
            (await open.Should().ThrowAsync<InvalidOperationException>())
                .WithMessage("*--max-sample 0*--schema-load*--column-types*");
        }

        var optionsNegative = new XmlReaderOptions { Path = "//Item", MaxSample = -1 };
        await using (var reader = new XmlStreamReader(_testXmlPath, optionsNegative))
        {
            var open = async () => await reader.OpenAsync();
            (await open.Should().ThrowAsync<InvalidOperationException>())
                .WithMessage("*--max-sample -1*--schema-load*--column-types*");
        }
    }

    [Fact]
    public async Task XmlStreamReader_MaxSample_Zero_WithDeclaredSchema_SucceedsWithoutInference()
    {
        var content = """
            <Root>
              <Item>
                <Id>101</Id>
                <Title>Item 1</Title>
              </Item>
            </Root>
            """;
        await File.WriteAllTextAsync(_testXmlPath, content);

        var options = new XmlReaderOptions
        {
            Path = "//Item",
            MaxSample = 0,
            ColumnTypes = "Id:int32,Title:string"
        };
        await using var reader = new XmlStreamReader(_testXmlPath, options);
        await reader.OpenAsync();

        reader.Columns.Should().NotBeNull();
        reader.Columns!.Select(c => c.Name).Should().Equal("Id", "Title");
        reader.Columns!.First(c => c.Name == "Id").ClrType.Should().Be(typeof(int));
    }

    [Fact]
    public async Task XmlStreamReader_MaxSample_CliExecutionHonorsFlag()
    {
        var content = """
            <Root>
              <Item>
                <Id>101</Id>
                <Title>Item 1</Title>
              </Item>
            </Root>
            """;
        await File.WriteAllTextAsync(_testXmlPath, content);
        var outPath = Path.Combine(Path.GetTempPath(), $"test_xml_cli_{Guid.NewGuid()}.csv");

        try
        {
            // MaxSample = 0 without declared schema fails with non-zero exit code
            var exitFail = await Program.Main(["-i", $"xml:{_testXmlPath}", "--path", "//Item", "--max-sample", "0", "-o", $"csv:{outPath}", "--no-stats"]);
            exitFail.Should().NotBe(0);

            // MaxSample = 0 with explicit --column-types succeeds
            var exitSuccess = await Program.Main(["-i", $"xml:{_testXmlPath}", "--path", "//Item", "--max-sample", "0", "--column-types", "Id:int32,Title:string", "-o", $"csv:{outPath}", "--no-stats"]);
            exitSuccess.Should().Be(0);
            File.Exists(outPath).Should().BeTrue();
        }
        finally
        {
            if (File.Exists(outPath)) File.Delete(outPath);
        }
    }
}
