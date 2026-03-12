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
			var jobs = JobFileParser.Parse(tempFile);
			var job = jobs["main"];

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
			var jobs = JobFileParser.Parse(tempFile);
			var job = jobs["main"];

			// Assert
			job.Input.Should().BeNull();
			job.Output.Should().Be("dummy.parquet");
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}

	[Fact]
	public void Parse_ShouldHandleMultiBranchDag()
	{
		// Arrange
		var yaml = @"
p:
  input: data.parquet
c:
  input: data.csv
joined:
  xstreamer: fusion-engine
  main: p
  ref: [c]
  query: SELECT * FROM p JOIN c ON p.id = c.id
";
		var tempFile = Path.GetTempFileName();
		File.WriteAllText(tempFile, yaml);

		try
		{
			// Act
			var jobs = JobFileParser.Parse(tempFile);

			// Assert
			jobs.Should().HaveCount(3);
			jobs["p"].Input.Should().Be("data.parquet");
			jobs["joined"].Xstreamer.Should().Be("fusion-engine");
			jobs["joined"].Main.Should().Be("p");
			jobs["joined"].Ref.Should().Contain("c");
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}
}
