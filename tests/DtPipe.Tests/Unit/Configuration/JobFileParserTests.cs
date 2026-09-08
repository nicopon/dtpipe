using DtPipe.Configuration;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using AwesomeAssertions;
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

	/// <summary>
	/// A key this loader does not know is refused. This fixture itself carried one for a cycle —
	/// 'sql:' at branch level, where the query belongs under 'provider-options' — and the dropped
	/// key went unnoticed because the assertions only looked at 'from' and 'ref'.
	/// </summary>
	[Fact]
	public void Parse_ShouldRefuse_AKeyItDoesNotKnow()
	{
		var act = () => JobFileParser.ParseContent(@"
joined:
  sql: SELECT 1
  from: p
");
		act.Should().Throw<YamlDotNet.Core.YamlException>().WithMessage("*sql*");
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
  provider-options:
    sql:
      query: SELECT * FROM p JOIN c ON p.id = c.id
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
	public void Parse_ShouldInterpolateCursorExpression()
	{
		// Arrange
		var path = Path.Combine(Path.GetTempPath(), "test_yaml_cursor_" + Guid.NewGuid().ToString("N") + ".sync").Replace("\\", "/");
		var cursor = new DtPipe.Core.Cursor.CursorValue("updated_at", "2026-06-16T12:00:00Z", DtPipe.Core.Cursor.CursorType.DateTime);
		var meta = new DtPipe.Core.Cursor.CursorRunMetadata(DateTime.UtcNow, DateTime.UtcNow, 100, "success");
		DtPipe.Core.Cursor.CursorStateStore.Save(path, cursor, meta);

		var yaml = $@"main:
  input: select * from t where updated_at >= '${{{{cursor://{path}|1970-01-01}}}}'
  output: dummy.parquet
";
		var tempFile = Path.GetTempFileName();
		File.WriteAllText(tempFile, yaml);

		try
		{
			// Act
			var jobs = JobFileParser.Parse(tempFile, null);
			var job = jobs["main"];

			// Assert
			job.Input.Should().Be("select * from t where updated_at >= '2026-06-16T12:00:00Z'");
		}
		finally
		{
			if (File.Exists(tempFile)) File.Delete(tempFile);
			if (File.Exists(path)) File.Delete(path);
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

	[Fact]
	public void Parse_ShouldSucceed_WhenUsingMemoryMappedFile()
	{
		// Arrange
		var yaml = @"main:
  input: memory-dummy.csv
  output: memory-dummy.parquet
";
		var jobName = "test-job-" + Guid.NewGuid().ToString("N");
		var tempPath = Path.Combine(Path.GetTempPath(), "dtpipe-job-" + jobName + ".yaml");
		File.WriteAllText(tempPath, yaml);

		try
		{
			// Act
			var jobs = JobFileParser.Parse($"memory://{jobName}");
			var job = jobs["main"];

			// Assert
			job.Input.Should().Be("memory-dummy.csv");
			job.Output.Should().Be("memory-dummy.parquet");
		}
		finally
		{
			if (File.Exists(tempPath)) File.Delete(tempPath);
		}
	}
}


[Collection("console-serial")]
public class YamlCliParityTests
{
	/// <summary>
	/// F8 — the YAML path and the CLI path must produce identical options objects.
	/// Drives ProviderConfigurationService both ways with stub csv contributors and
	/// compares the registered CsvReaderOptions / CsvWriterOptions.
	/// </summary>
	[Fact]
	public async Task Yaml_And_Cli_Produce_Identical_Options()
	{
		var registry = new DtPipe.Core.Options.OptionsRegistry();
		var contributors = YamlParityStubs.Build();

		// ── YAML path: provider-options dictionaries ──
		var yamlJob = new DtPipe.Core.Models.JobDefinition
		{
			Input = "in.csv",
			Output = "out.csv",
			ProviderOptions = new()
			{
				["csv"] = new() { ["separator"] = ";", ["column-types"] = "Id:int32" },
				["csv-writer"] = new() { ["separator"] = "|" }
			}
		};
		registry.BeginScope();
		var yamlStderr = CaptureStderr(() => new DtPipe.Cli.Services.ProviderConfigurationService(contributors, registry)
			.BindOptions(yamlJob, context: null));
		var yamlReader = registry.Get<DtPipe.Adapters.Csv.CsvReaderOptions>();
		var yamlWriter = registry.Get<DtPipe.Adapters.Csv.CsvWriterOptions>();
		yamlStderr.Should().NotContain("[dtpipe] Warning",
			"the shared 'csv:' key feeds both sides — reader-only keys must be skipped silently on the writer");

		// ── CLI path: stage-scoped args ──
		var cliJob = new DtPipe.Core.Models.JobDefinition { Input = "in.csv", Output = "out.csv" };
		var cliContext = new DtPipe.Cli.Pipeline.CliJobContext(
			ReaderArguments: new[] { "--csv-separator", ";", "--column-types", "Id:int32" },
			PipelineArguments: System.Array.Empty<string>(),
			WriterArguments: new[] { "-o", "out.csv", "--csv-separator", "|" },
			Arguments: System.Array.Empty<string>());
		registry.BeginScope();
		new DtPipe.Cli.Services.ProviderConfigurationService(contributors, registry)
			.BindOptions(cliJob, cliContext);
		var cliReader = registry.Get<DtPipe.Adapters.Csv.CsvReaderOptions>();
		var cliWriter = registry.Get<DtPipe.Adapters.Csv.CsvWriterOptions>();

		// ── Compare ──
		cliReader.Separator.Should().Be(yamlReader.Separator).And.Be(";");
		cliReader.ColumnTypes.Should().Be(yamlReader.ColumnTypes).And.Be("Id:int32");
		cliWriter.Separator.Should().Be(yamlWriter.Separator).And.Be("|");
		// Note: CsvWriterOptions has no ColumnTypes property at all — before the
		// shared-key silence rule, binding the block onto the writer warned on it.
	}

	private static string CaptureStderr(Action action)
	{
		var original = Console.Error;
		var captured = new StringWriter();
		Console.SetError(captured);
		try { action(); }
		finally { Console.SetError(original); }
		return captured.ToString();
	}

	private static class YamlParityStubs
	{
		public sealed class ReaderStubFactory : IStreamReaderFactory, ICliContributor
		{
			public string ComponentName => "csv";
			public string Category => "Readers";
			public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvReaderOptions);
			public bool CanHandle(string connectionString) => connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
			public IEnumerable<FlagDef> GetFlagDefs() => CliOptionBuilder.GenerateFlagDefsForType(OptionsType);
			public DtPipe.Core.Abstractions.IStreamReader Create(DtPipe.Core.Options.OptionsRegistry registry) => throw new NotSupportedException();
			public IEnumerable<Type> GetSupportedOptionTypes() => new[] { OptionsType };
			public bool RequiresQuery => false;
		}

		public sealed class WriterStubFactory : IDataWriterFactory, ICliContributor
		{
			public string ComponentName => "csv";
			public string Category => "Writers";
			public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvWriterOptions);
			public bool CanHandle(string connectionString) => connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
			public IEnumerable<FlagDef> GetFlagDefs() => CliOptionBuilder.GenerateFlagDefsForType(OptionsType);
			public DtPipe.Core.Abstractions.IDataWriter Create(DtPipe.Core.Options.OptionsRegistry registry) => throw new NotSupportedException();
			public IEnumerable<Type> GetSupportedOptionTypes() => new[] { OptionsType };
		}

		public static IEnumerable<ICliContributor> Build() => new ICliContributor[]
		{
			new ReaderStubFactory(),
			new WriterStubFactory(),
		};
	}
}

public class InterpolationUnificationTests
{
    /// <summary>F11 — YAML interpolation routes through the canonical resolver chain (env).</summary>
    [Fact]
    public void Env_Interpolation_Routes_Through_Resolver()
    {
        Environment.SetEnvironmentVariable("DTPIPE_TEST_INTERP_VAR", "resolved.csv");
        try
        {
            var yaml = "main:\n  input: ${{DTPIPE_TEST_INTERP_VAR}}\n  output: out.csv\n";
            var jobs = JobFileParser.ParseContent(yaml);
            Assert.Equal("resolved.csv", jobs["main"].Input);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_TEST_INTERP_VAR", null);
        }
    }

    /// <summary>F11 — unknown variables are left verbatim (composite semantics).</summary>
    [Fact]
    public void Unknown_Var_Left_Verbatim()
    {
        var yaml = "main:\n  input: ${{DTPIPE_DEFINITELY_UNSET_VAR_42}}\n  output: out.csv\n";
        var jobs = JobFileParser.ParseContent(yaml);
        Assert.Equal("${{DTPIPE_DEFINITELY_UNSET_VAR_42}}", jobs["main"].Input);
    }
}
