using DtPipe.Core.Services;
using DtPipe.Transformers.Script;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

public class ComputeDataTransformerFactoryTests
{
	private class TestableComputeDataTransformerFactory : ComputeDataTransformerFactory
	{
		public TestableComputeDataTransformerFactory() : base(new DtPipe.Core.Options.OptionsRegistry(), new JsEngineProvider()) { }
	}

	[Fact]
	public void CreateFromConfiguration_ShouldLoadImplicitFile_WhenFileExists()
	{
		// Arrange
		var tempFile = Path.GetTempFileName();
		var scriptContent = "return row.NAME.toUpperCase();";
		File.WriteAllText(tempFile, scriptContent);

		var factory = new TestableComputeDataTransformerFactory();
		var config = new List<(string, string)> { ("--script", $"NAME:{tempFile}") };

		try
		{
			// Act
			var transformer = (ComputeDataTransformer)factory.CreateFromConfiguration(config);

			// Assert
			transformer.Should().NotBeNull();
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}

	[Fact]
	public void CreateFromConfiguration_ShouldLoadExplicitFile_WhenFileExists()
	{
		// Arrange
		var tempFile = Path.GetTempFileName();
		var scriptContent = "return row.NAME.toUpperCase();";
		File.WriteAllText(tempFile, scriptContent);

		var factory = new TestableComputeDataTransformerFactory();
		// Use @ prefix
		var config = new List<(string, string)> { ("--script", $"NAME:@{tempFile}") };

		try
		{
			// Act
			var transformer = (ComputeDataTransformer)factory.CreateFromConfiguration(config);

			// Assert
			transformer.Should().NotBeNull();
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}

	[Fact]
	public void CreateFromConfiguration_ShouldUseInlineScript_WhenFileDoesNotExist()
	{
		// Arrange
		var inlineScript = "return row.AGE * 2;";
		var factory = new TestableComputeDataTransformerFactory();
		var config = new List<(string, string)> { ("--script", $"AGE:{inlineScript}") };

		// Act
		var transformer = (ComputeDataTransformer)factory.CreateFromConfiguration(config);

		// Assert
		transformer.Should().NotBeNull();
	}

	[Fact]
	public void CreateFromConfiguration_ShouldSupportComputeAlias()
	{
		// Arrange
		var inlineScript = "return row.AGE * 2;";
		var factory = new TestableComputeDataTransformerFactory();
		var config = new List<(string, string)> { ("--compute", $"AGE:{inlineScript}") };

		// Act
		var transformer = (ComputeDataTransformer)factory.CreateFromConfiguration(config);

		// Assert
		transformer.Should().NotBeNull();
	}
}
