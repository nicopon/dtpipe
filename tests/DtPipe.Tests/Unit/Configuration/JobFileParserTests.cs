using DtPipe.Configuration;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Configuration;

public class JobFileParserTests
{
	[Fact]
	public void Parse_ShouldSucceed_WhenQueryIsMissing()
	{
		// Arrange
		var yaml = @"
input: dummy.csv
output: dummy.parquet
";
		var tempFile = Path.GetTempFileName();
		File.WriteAllText(tempFile, yaml);

		try
		{
			// Act
			var job = JobFileParser.Parse(tempFile);

			// Assert
			job.Query.Should().BeNull();
			job.Input.Should().Be("dummy.csv");
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}

	[Fact]
	public void Parse_ShouldSucceed_EvenWhenInputIsMissing()
	{
		// Arrange: Partial job (template)
		var yaml = @"
output: dummy.parquet
";
		var tempFile = Path.GetTempFileName();
		File.WriteAllText(tempFile, yaml);

		try
		{
			// Act
			var job = JobFileParser.Parse(tempFile);

			// Assert
			job.Input.Should().BeNull();
			job.Output.Should().Be("dummy.parquet");
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}
}
