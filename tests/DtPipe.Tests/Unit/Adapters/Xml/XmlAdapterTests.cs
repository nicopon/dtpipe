using System.Xml.Linq;
using DtPipe.Adapters.Xml;
using DtPipe.Core.Models;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Xml;

public class XmlAdapterTests : IAsyncLifetime
{
	private string _testXmlPath = null!;

	public ValueTask InitializeAsync()
	{
		_testXmlPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.xml");
		return ValueTask.CompletedTask;
	}

	public ValueTask DisposeAsync()
	{
		if (File.Exists(_testXmlPath)) File.Delete(_testXmlPath);
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task XmlStreamReader_ShouldReadAttributesAndElements()
	{
		// Arrange
		var content = """
            <Catalog>
              <Item id="101" type="book">
                <Title>Pro Git</Title>
                <Price>0.00</Price>
              </Item>
              <Item id="102" type="magazine">
                <Title>MSDN Magazine</Title>
                <Price>5.99</Price>
              </Item>
            </Catalog>
            """;
		await File.WriteAllTextAsync(_testXmlPath, content);

		var options = new XmlReaderOptions { Path = "//Item" };
		var reader = new XmlStreamReader(_testXmlPath, options);

		// Act
		await reader.OpenAsync();
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach(var row in batch.ToArray()) rows.Add(row);
		}
		await reader.DisposeAsync();

		// Assert
		reader.Columns.Should().HaveCount(4);
		reader.Columns.Select(c => c.Name).Should().Contain(new[] { "_id", "_type", "Title", "Price" });

		rows.Should().HaveCount(2);
		
		var row1 = rows[0];
		// Order of columns depends on the first record
		var idIdx = reader.Columns!.ToList().FindIndex(c => c.Name == "_id");
		var titleIdx = reader.Columns!.ToList().FindIndex(c => c.Name == "Title");
		
		row1[idIdx].Should().Be("101");
		row1[titleIdx].Should().Be("Pro Git");
	}

	[Fact]
	public async Task XmlStreamReader_ShouldSupportDeepSearch()
	{
		// Arrange
		var content = """
            <Root>
              <Level1>
                <Item id="1" />
              </Level1>
              <Level1>
                <SubLevel>
                  <Item id="2" />
                </SubLevel>
              </Level1>
            </Root>
            """;
		await File.WriteAllTextAsync(_testXmlPath, content);

		var options = new XmlReaderOptions { Path = "//Item" };
		var reader = new XmlStreamReader(_testXmlPath, options);

		// Act
		await reader.OpenAsync();
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach(var row in batch.ToArray()) rows.Add(row);
		}
		await reader.DisposeAsync();

		// Assert
		rows.Should().HaveCount(2);
	}

	[Fact]
	public async Task XmlStreamReader_ShouldHandleNestedStructures()
	{
		// Arrange
		var content = """
            <Root>
              <Item>
                <User>
                  <Name>Alice</Name>
                  <Role>Admin</Role>
                </User>
              </Item>
            </Root>
            """;
		await File.WriteAllTextAsync(_testXmlPath, content);

		var options = new XmlReaderOptions { Path = "//Item" };
		var reader = new XmlStreamReader(_testXmlPath, options);

		// Act
		await reader.OpenAsync();
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach(var row in batch.ToArray()) rows.Add(row);
		}
		await reader.DisposeAsync();

		// Assert
		rows.Should().HaveCount(1);
		var user = rows[0][0] as Dictionary<string, object?>;
		user.Should().NotBeNull();
		user!["Name"].Should().Be("Alice");
		user!["Role"].Should().Be("Admin");
	}

	[Fact]
	public async Task XmlStreamReader_ShouldHandleRepeatedElementsAsLists()
	{
		// Arrange
		var content = """
            <Root>
              <Item>
                <Tag>A</Tag>
                <Tag>B</Tag>
                <Tag>C</Tag>
              </Item>
            </Root>
            """;
		await File.WriteAllTextAsync(_testXmlPath, content);

		var options = new XmlReaderOptions { Path = "//Item" };
		var reader = new XmlStreamReader(_testXmlPath, options);

		// Act
		await reader.OpenAsync();
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach(var row in batch.ToArray()) rows.Add(row);
		}
		await reader.DisposeAsync();

		// Assert
		rows.Should().HaveCount(1);
		var tags = rows[0][0] as List<object?>;
		tags.Should().NotBeNull();
		tags.Should().HaveCount(3);
		tags.Should().Contain(new[] { "A", "B", "C" });
	}

	[Fact]
	public async Task XmlStreamReader_ShouldHandleMixedContent()
	{
		// Arrange
		var content = """
            <Root>
              <Item id="1">
                Some text before
                <Child>Value</Child>
                Some text after
              </Item>
            </Root>
            """;
		await File.WriteAllTextAsync(_testXmlPath, content);

		var options = new XmlReaderOptions { Path = "//Item" };
		var reader = new XmlStreamReader(_testXmlPath, options);

		// Act
		await reader.OpenAsync();
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach(var row in batch.ToArray()) rows.Add(row);
		}
		await reader.DisposeAsync();

		// Assert
		rows.Should().HaveCount(1);
		// In my current implementation, mixed text at the root of the record node is currently ignored 
		// if elements are present, or put in _value if only text.
		// Let's verify that child node is captured.
		var dict = new Dictionary<string, object?>();
		for(int i=0; i<reader.Columns!.Count; i++) dict[reader.Columns[i].Name] = rows[0][i];
		
		dict["Child"].Should().Be("Value");
		dict["_id"].Should().Be("1");
	}

	[Fact]
	public async Task XmlStreamReader_ShouldMatchAbsolutePath()
	{
		// Arrange
		var content = """
            <Root>
              <NotHere>
                <Item id="1" />
              </NotHere>
              <Here>
                <Items>
                  <Item id="2" />
                </Items>
              </Here>
            </Root>
            """;
		await File.WriteAllTextAsync(_testXmlPath, content);

		var options = new XmlReaderOptions { Path = "Root/Here/Items/Item" };
		var reader = new XmlStreamReader(_testXmlPath, options);

		// Act
		await reader.OpenAsync();
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach (var row in batch.ToArray()) rows.Add(row);
		}
		await reader.DisposeAsync();

		// Assert
		rows.Should().HaveCount(1);
		
		var idIdx = reader.Columns!.ToList().FindIndex(c => c.Name == "_id");
		rows[0][idIdx].Should().Be("2"); // Only id=2 matches the strict path
	}

	[Fact]
	public async Task XmlStreamReader_ShouldHandleListOfComplexObjects()
	{
		// Arrange
		var content = """
            <Root>
              <Record>
                <Employees>
                  <Employee id="1"><Name>Alice</Name></Employee>
                  <Employee id="2"><Name>Bob</Name></Employee>
                </Employees>
              </Record>
            </Root>
            """;
		await File.WriteAllTextAsync(_testXmlPath, content);

		var options = new XmlReaderOptions { Path = "//Record" };
		var reader = new XmlStreamReader(_testXmlPath, options);

		// Act
		await reader.OpenAsync();
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach (var row in batch.ToArray()) rows.Add(row);
		}
		await reader.DisposeAsync();

		// Assert
		rows.Should().HaveCount(1);
		
		var cols = reader.Columns!.ToList();
		var empIdx = cols.FindIndex(c => c.Name == "Employees");
		
		var wrapper = rows[0][empIdx] as Dictionary<string, object?>;
		wrapper.Should().NotBeNull();
		
		var list = wrapper!["Employee"] as List<object?>;
		list.Should().NotBeNull();
		list.Should().HaveCount(2);

		var firstEmp = list![0] as Dictionary<string, object?>;
		firstEmp!["_id"].Should().Be("1");
		firstEmp!["Name"].Should().Be("Alice");
	}

	[Fact]
	public async Task XmlStreamReader_ShouldHandleNamespacesAndPrefixesGracefully()
	{
		// Arrange
		var content = """
            <ns1:Root xmlns:ns1="http://example.com/ns1" xmlns:ns2="http://example.com/ns2">
              <ns1:Item ns2:id="1">
                <ns2:Value>Hello</ns2:Value>
              </ns1:Item>
            </ns1:Root>
            """;
		await File.WriteAllTextAsync(_testXmlPath, content);

		// Current implementation simplifies matching to LocalName, so we look for "Item"
		var options = new XmlReaderOptions { Path = "//Item" };
		var reader = new XmlStreamReader(_testXmlPath, options);

		// Act
		await reader.OpenAsync();
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach (var row in batch.ToArray()) rows.Add(row);
		}
		await reader.DisposeAsync();

		// Assert
		rows.Should().HaveCount(1);
		
		var cols = reader.Columns!.Select(c => c.Name).ToList();
		// It should find Value (local name) and _id (attribute local name)
		cols.Should().Contain("Value");
		cols.Should().Contain("_id");

		var valIdx = cols.IndexOf("Value");
		var idIdx = cols.IndexOf("_id");

		rows[0][valIdx].Should().Be("Hello");
		rows[0][idIdx].Should().Be("1");
	}
}
