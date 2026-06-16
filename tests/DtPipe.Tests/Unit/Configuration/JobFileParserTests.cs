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
		var yaml = @"main:
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
		var yaml = @"main:
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
  sql: SELECT * FROM p JOIN c ON p.id = c.id
  from: p
  ref: [c]
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
			jobs["joined"].From.Should().Be("p");
			jobs["joined"].Ref.Should().Contain("c");
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}

	[Fact]
	public void Parse_ShouldInterpolateEnvironmentVariables()
	{
		// Arrange
		Environment.SetEnvironmentVariable("DTPIPE_TEST_ENV_VAR", "my-env-value");
		var yaml = @"main:
  input: ${{DTPIPE_TEST_ENV_VAR}}
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
			job.Input.Should().Be("my-env-value");
		}
		finally
		{
			Environment.SetEnvironmentVariable("DTPIPE_TEST_ENV_VAR", null);
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}

	[Fact]
	public void Parse_ShouldInterpolateKeyringSecrets()
	{
		// Arrange
		var secretsManager = new DtPipe.Cli.Security.InMemorySecretsManager();
		secretsManager.SetSecret("test-secret", "my-secret-value");

		var yaml = @"main:
  input: ${{keyring://test-secret}}
  output: dummy.parquet
";
		var tempFile = Path.GetTempFileName();
		File.WriteAllText(tempFile, yaml);

		try
		{
			// Act
			var jobs = JobFileParser.Parse(tempFile, secretsManager);
			var job = jobs["main"];

			// Assert
			job.Input.Should().Be("my-secret-value");
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}

	[Fact]
	public void Parse_ShouldNotBreakYaml_WhenInterpolatedValueContainsYamlSpecialCharacters()
	{
		// Arrange
		var secretsManager = new DtPipe.Cli.Security.InMemorySecretsManager();
		// A secret containing quotes, colons, and newlines that would normally break raw YAML parsing
		var complexSecret = "my:complex:string\nwith \"quotes\" and 'single' quotes and trailing: colon";
		secretsManager.SetSecret("complex-secret", complexSecret);

		var yaml = @"main:
  input: ${{keyring://complex-secret}}
  output: dummy.parquet
";
		var tempFile = Path.GetTempFileName();
		File.WriteAllText(tempFile, yaml);

		try
		{
			// Act
			var jobs = JobFileParser.Parse(tempFile, secretsManager);
			var job = jobs["main"];

			// Assert
			job.Input.Should().Be(complexSecret);
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}

	[Fact]
	public void Parse_ShouldInterpolateInsideTransformers()
	{
		// Arrange
		var secretsManager = new DtPipe.Cli.Security.InMemorySecretsManager();
		secretsManager.SetSecret("trans-secret", "secret-trans-val");
		Environment.SetEnvironmentVariable("DTPIPE_TEST_TRANS_ENV", "env-trans-val");

		var yaml = @"main:
  input: dummy.csv
  transformers:
    - type: compute
      mappings:
        Val: ${{DTPIPE_TEST_TRANS_ENV}}
      options:
        some_opt: ${{keyring://trans-secret}}
";
		var tempFile = Path.GetTempFileName();
		File.WriteAllText(tempFile, yaml);

		try
		{
			// Act
			var jobs = JobFileParser.Parse(tempFile, secretsManager);
			var job = jobs["main"];
			var config = job.Transformers.Should().ContainSingle().Subject;

			// Assert
			config.Mappings.Should().ContainKey("Val");
			config.Mappings["Val"].Should().Be("env-trans-val");

			config.Options.Should().ContainKey("some_opt");
			config.Options["some_opt"].Should().Be("secret-trans-val");
		}
		finally
		{
			Environment.SetEnvironmentVariable("DTPIPE_TEST_TRANS_ENV", null);
			if (File.Exists(tempFile)) File.Delete(tempFile);
		}
	}
}

