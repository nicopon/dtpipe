using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Cli.Services;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Services;
using Apache.Arrow;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class OrderedPipelineTests
{
	private readonly Mock<IDataTransformerFactory> _fakeFactory;
	private readonly Mock<IDataTransformerFactory> _nullFactory;
	private readonly Mock<IDataTransformerFactory> _formatFactory;
	private readonly Mock<IDataTransformerFactory> _staticFactory;
	private readonly List<IDataTransformerFactory> _factories;

	public OrderedPipelineTests()
	{
		_fakeFactory = new Mock<IDataTransformerFactory>();
		SetupFactory(_fakeFactory, "--fake", FlagArity.Scalar, "-f");

		_nullFactory = new Mock<IDataTransformerFactory>();
		SetupFactory(_nullFactory, "--null", FlagArity.Scalar);

		_formatFactory = new Mock<IDataTransformerFactory>();
		SetupFactory(_formatFactory, "--format", FlagArity.Scalar);

		_staticFactory = new Mock<IDataTransformerFactory>();
		SetupFactory(_staticFactory, "--overwrite", FlagArity.Scalar);

		_factories = new List<IDataTransformerFactory>
		{
			_fakeFactory.Object,
			_nullFactory.Object,
			_formatFactory.Object,
			_staticFactory.Object
		};

		// Setup OptionsType for each factory to prevent Activator.CreateInstance failure
		_fakeFactory.Setup(f => f.OptionsType).Returns(typeof(DtPipe.Transformers.Arrow.Fake.FakeOptions));
		_nullFactory.Setup(f => f.OptionsType).Returns(typeof(DtPipe.Transformers.Arrow.Null.NullOptions));
		_formatFactory.Setup(f => f.OptionsType).Returns(typeof(DtPipe.Transformers.Arrow.Format.FormatOptions));
		_staticFactory.Setup(f => f.OptionsType).Returns(typeof(DtPipe.Transformers.Arrow.Overwrite.OverwriteOptions));
	}

	private void SetupFactory<T>(Mock<T> mock, string mainAlias, FlagArity arity, params string[] aliases) where T : class, IDataTransformerFactory
	{
		var flagDef = new FlagDef(mainAlias, aliases, arity, FlagScope.PerBranch, mainAlias.TrimStart('-'));

		mock.Setup(f => f.ComponentName).Returns(mainAlias.TrimStart('-'));
		mock.As<ICliContributor>().Setup(f => f.GetFlagDefs()).Returns(new List<FlagDef> { flagDef });
	}

	[Fact]
	public void Build_ShouldPreserveOrder_WhenDifferentTransformersAreInterleaved()
	{
		// Arrange
		var builder = new TransformerPipelineBuilder(_factories);
		var args = new[]
		{
			"--fake", "NAME:name.fullName",
			"--null", "SENSITIVE_DATA",
			"--fake", "EMAIL:internet.email",
			"--format", "DISPLAY:{NAME} <{EMAIL}>"
		};

		var fakeT1 = new Mock<IDataTransformer>();
		var fakeT2 = new Mock<IDataTransformer>();
		var nullT = new Mock<IDataTransformer>();
		var formatT = new Mock<IDataTransformer>();

		_fakeFactory.Setup(f => f.CreateFromOptions(It.Is<DtPipe.Transformers.Arrow.Fake.FakeOptions>(o => o.Fake.Contains("NAME:name.fullName"))))
			.Returns(fakeT1.Object);

		_nullFactory.Setup(f => f.CreateFromOptions(It.IsAny<DtPipe.Transformers.Arrow.Null.NullOptions>()))
			.Returns(nullT.Object);

		_fakeFactory.Setup(f => f.CreateFromOptions(It.Is<DtPipe.Transformers.Arrow.Fake.FakeOptions>(o => o.Fake.Contains("EMAIL:internet.email"))))
			.Returns(fakeT2.Object);

		_formatFactory.Setup(f => f.CreateFromOptions(It.IsAny<DtPipe.Transformers.Arrow.Format.FormatOptions>()))
			.Returns(formatT.Object);

		// Act
		var pipeline = builder.Build(args);

		// Assert
		pipeline.Should().HaveCount(4);
		pipeline[0].Should().Be(fakeT1.Object);
		pipeline[1].Should().Be(nullT.Object);
		pipeline[2].Should().Be(fakeT2.Object);
		pipeline[3].Should().Be(formatT.Object);
	}

	[Fact]
	public void Build_ShouldGroupConsecutiveTransformers_OfTheSameType()
	{
		// Arrange
		var builder = new TransformerPipelineBuilder(_factories);
		var args = new[]
		{
			"--fake", "A:a",
			"--fake", "B:b", // Should group with A
            "--null", "C",
			"--fake", "D:d"  // Should NOT group
        };

		var fakeGroup1 = new Mock<IDataTransformer>();
		var fakeGroup1bis = new Mock<IDataTransformer>();
		var fakeGroup2 = new Mock<IDataTransformer>();
		var nullGroup = new Mock<IDataTransformer>();

		// Expected behavior: every --fake is a trigger and creates a new instance if --fake was already seen in current group
		_fakeFactory.Setup(f => f.CreateFromOptions(It.Is<DtPipe.Transformers.Arrow.Fake.FakeOptions>(o => o.Fake.Count == 1 && o.Fake.Contains("A:a"))))
			.Returns(fakeGroup1.Object);

		_fakeFactory.Setup(f => f.CreateFromOptions(It.Is<DtPipe.Transformers.Arrow.Fake.FakeOptions>(o => o.Fake.Count == 1 && o.Fake.Contains("B:b"))))
			.Returns(fakeGroup1bis.Object);

		_nullFactory.Setup(f => f.CreateFromOptions(It.Is<DtPipe.Transformers.Arrow.Null.NullOptions>(o => o.Columns.Contains("C"))))
			.Returns(nullGroup.Object);

		_fakeFactory.Setup(f => f.CreateFromOptions(It.Is<DtPipe.Transformers.Arrow.Fake.FakeOptions>(o => o.Fake.Count == 1 && o.Fake.Contains("D:d"))))
			.Returns(fakeGroup2.Object);

		// Act
		var pipeline = builder.Build(args);

		// Assert
		pipeline.Should().HaveCount(4);
		pipeline[0].Should().Be(fakeGroup1.Object);    // Fake [A]
		pipeline[1].Should().Be(fakeGroup1bis.Object); // Fake [B]
		pipeline[2].Should().Be(nullGroup.Object);     // Null [C]
		pipeline[3].Should().Be(fakeGroup2.Object);    // Fake [D]
	}

	[Fact]
	public void Build_ShouldHandleFlags_WithoutConsumingNextToken()
	{
		// Arrange
		var builder = new TransformerPipelineBuilder(_factories);

		// Setup --skip-null as a FLAG (Boolean arity) for Fake factory
		var skipNullFlag = new FlagDef("--skip-null", System.Array.Empty<string>(), FlagArity.Boolean, FlagScope.PerBranch, "fake");
		var fakeFlag = new FlagDef("--fake", System.Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "fake");

		_fakeFactory.As<ICliContributor>().Setup(f => f.GetFlagDefs()).Returns(new List<FlagDef> { fakeFlag, skipNullFlag });

		var args = new[]
		{
			"--skip-null",      // Should be treated as flag (value=true implicit)
            "--fake", "Value"   // Should NOT be consumed by skip-null
        };

		var fakeT = new Mock<IDataTransformer>();

		// Expectation: CreateFromOptions called with SkipNull=true and Fake=Value in the same group (same factory)
		_fakeFactory.Setup(f => f.CreateFromOptions(It.Is<DtPipe.Transformers.Arrow.Fake.FakeOptions>(o =>
			o.SkipNull == true &&
			o.Fake.Contains("Value"))))
			.Returns(fakeT.Object);

		// Act
		var pipeline = builder.Build(args);

		// Assert
		pipeline.Should().HaveCount(1);
		pipeline[0].Should().Be(fakeT.Object);
	}

	[Fact]
	public async Task StreamTransformerReaderAdapter_ReadBatchesAsync_ShouldConvertColumnarToRows()
	{
		// Arrange
		var mockTransformer = new Mock<IStreamTransformer>();
		
		var schemaBuilder = new Apache.Arrow.Schema.Builder()
			.Field(f => f.Name("Id").DataType(Apache.Arrow.Types.Int64Type.Default).Nullable(false))
			.Field(f => f.Name("Value").DataType(Apache.Arrow.Types.StringType.Default).Nullable(true));
		var schema = schemaBuilder.Build();
		
		var columns = new List<PipeColumnInfo>
		{
			new("Id", typeof(long), false),
			new("Value", typeof(string), true)
		};

		mockTransformer.Setup(t => t.Schema).Returns(schema);
		mockTransformer.Setup(t => t.Columns).Returns(columns);

		var idBuilder = new Int64Array.Builder().Append(1).Append(2).Append(3);
		var valueBuilder = new StringArray.Builder().Append("A").Append("B").AppendNull();
		var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build(), valueBuilder.Build() }, 3);

		mockTransformer.Setup(t => t.ReadResultsAsync(It.IsAny<IAsyncEnumerable<RecordBatch>>(), It.IsAny<CancellationToken>()))
			.Returns(HelperAsyncEnumerable(batch));

		var adapter = new StreamTransformerReaderAdapter(mockTransformer.Object);
		var reader = adapter.Create(new OptionsRegistry());

		// Act
		var rows = new List<object?[]>();
		await foreach (var batchChunk in reader.ReadBatchesAsync(2))
		{
			for (int i = 0; i < batchChunk.Length; i++)
			{
				rows.Add(batchChunk.Span[i]);
			}
		}

		// Assert
		rows.Should().HaveCount(3);
		rows[0].Should().Equal(1L, "A");
		rows[1].Should().Equal(2L, "B");
		rows[2].Should().Equal(3L, null);
	}

	// LinearPipelineService dispatch (CLAUDE.md engine-change obligation): a YAML branch with no
	// raw args must be routed through the CreateFromJob surface, never the CLI string[] surface.
	[Fact]
	public async Task ExecuteAsync_YamlBranch_NoRawArgs_UsesCreateFromJobSurface()
	{
		// Minimal DI graph (mirrors E2EIntegrationTests) so LinearPipelineService constructs
		// and ExecuteAsync reaches the stream-transformer dispatch.
		var registry = new OptionsRegistry();
		registry.Register(new DtPipe.Adapters.Generate.GenerateReaderOptions());
		registry.Register(new DtPipe.Adapters.Null.NullDataWriterOptions());

		var services = new ServiceCollection();
		services.AddLogging();
		services.AddSingleton(registry);
		services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(
			new DtPipe.Adapters.Generate.GenerateReaderDescriptor(), sp.GetRequiredService<OptionsRegistry>(), sp));
		services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(
			new DtPipe.Adapters.Null.NullDataWriterFactory(), sp.GetRequiredService<OptionsRegistry>(), sp));
		services.AddSingleton<IRowToColumnarBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory>();
		services.AddSingleton<IColumnarToRowBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory>();
		services.AddSingleton<ExportService>();
		services.AddSingleton<HookExecutor>();
		services.AddSingleton<MetricsService>();
		services.AddSingleton<SchemaValidationService>();
		services.AddSingleton<PipelineExecutor>();
		services.AddSingleton<DtPipe.Core.Abstractions.Dag.IMemoryChannelRegistry, DtPipe.Core.Pipelines.Dag.MemoryChannelRegistry>();

		var mockProgress = new Mock<IExportProgress>();
		mockProgress.Setup(p => p.GetMetrics()).Returns(new ExportMetrics(DateTime.UtcNow, DateTime.UtcNow, 0, 0, 0, 0, new Dictionary<string, long>()));
		var mockObserver = new Mock<IExportObserver>();
		mockObserver.Setup(o => o.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IReadOnlyList<(string Name, bool IsColumnar)>>(), It.IsAny<bool>(), It.IsAny<string?>(), It.IsAny<bool>()))
			.Returns(mockProgress.Object);
		services.AddSingleton(mockObserver.Object);

		// Seam under test: a concrete fake so the REAL default interface method IsApplicable(JobDefinition)
		// runs (ProviderOptions.ContainsKey("merge")); its YAML surface throws a sentinel we can detect.
		var sentinel = new InvalidOperationException("dispatch-reached-CreateFromJob");
		var fakeFactory = new ThrowingStreamTransformerFactory(sentinel);
		services.AddSingleton<IStreamTransformerFactory>(fakeFactory);

		var serviceProvider = services.BuildServiceProvider();

		var pipelineService = new LinearPipelineService(
			new List<ICliContributor>(),
			serviceProvider,
			serviceProvider.GetRequiredService<DtPipe.Core.Abstractions.Dag.IMemoryChannelRegistry>(),
			registry,
			Spectre.Console.AnsiConsole.Console);

		var job = new JobDefinition { Output = "null:", ProviderOptions = new() { ["merge"] = new() } };

		// The stream-transformer dispatch runs before ExecuteAsync's try/catch, so the sentinel propagates.
		var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
			pipelineService.ExecuteAsync(job, context: null, token: default));

		thrown.Should().BeSameAs(sentinel);
		fakeFactory.CreateFromJobCalls.Should().Be(1, "a YAML branch must use the CreateFromJob surface");
		fakeFactory.CliCreateCalls.Should().Be(0, "the CLI string[] surface must not be used for a YAML branch");
	}

	// LinearPipelineService failure reporting (CLAUDE.md engine-change obligation): a branch that
	// faults must leave the reason on the service, not only on the console. A caller that silences
	// the console to read a tool result got `exitCode: 1` with nothing to act on — and
	// validate-yaml-job now sends every caller to the tool that does exactly that.
	[Fact]
	public async Task ExecuteAsync_ABranchThatFaults_LeavesTheReasonOnTheService()
	{
		var registry = new OptionsRegistry();
		registry.Register(new DtPipe.Adapters.Generate.GenerateReaderOptions());
		registry.Register(new DtPipe.Adapters.Null.NullDataWriterOptions());

		var services = new ServiceCollection();
		services.AddLogging();
		services.AddSingleton(registry);
		services.AddSingleton<IStreamReaderFactory>(sp => new CliStreamReaderFactory(
			new DtPipe.Adapters.Generate.GenerateReaderDescriptor(), sp.GetRequiredService<OptionsRegistry>(), sp));
		services.AddSingleton<IDataWriterFactory>(sp => new CliDataWriterFactory(
			new DtPipe.Adapters.Null.NullDataWriterFactory(), sp.GetRequiredService<OptionsRegistry>(), sp));
		services.AddSingleton<IRowToColumnarBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowRowToColumnarBridgeFactory>();
		services.AddSingleton<IColumnarToRowBridgeFactory, DtPipe.Adapters.Infrastructure.Arrow.ArrowColumnarToRowBridgeFactory>();
		services.AddSingleton<ExportService>();
		services.AddSingleton<HookExecutor>();
		services.AddSingleton<MetricsService>();
		services.AddSingleton<SchemaValidationService>();
		services.AddSingleton<PipelineExecutor>();
		services.AddSingleton<DtPipe.Core.Abstractions.Dag.IMemoryChannelRegistry, DtPipe.Core.Pipelines.Dag.MemoryChannelRegistry>();
		services.AddSingleton<IEnumerable<IStreamTransformerFactory>>(System.Array.Empty<IStreamTransformerFactory>());

		var mockProgress = new Mock<IExportProgress>();
		mockProgress.Setup(p => p.GetMetrics()).Returns(new ExportMetrics(DateTime.UtcNow, DateTime.UtcNow, 0, 0, 0, 0, new Dictionary<string, long>()));
		var mockObserver = new Mock<IExportObserver>();
		mockObserver.Setup(o => o.CreateProgressReporter(It.IsAny<bool>(), It.IsAny<IReadOnlyList<(string Name, bool IsColumnar)>>(), It.IsAny<bool>(), It.IsAny<string?>(), It.IsAny<bool>()))
			.Returns(mockProgress.Object);
		services.AddSingleton(mockObserver.Object);

		var serviceProvider = services.BuildServiceProvider();
		var project = new CliDataTransformerFactory(new DtPipe.Transformers.Arrow.Project.ProjectDataTransformerFactory(registry));

		var pipelineService = new LinearPipelineService(
			new List<ICliContributor> { project },
			serviceProvider,
			serviceProvider.GetRequiredService<DtPipe.Core.Abstractions.Dag.IMemoryChannelRegistry>(),
			registry,
			Spectre.Console.AnsiConsole.Console);

		// The real shape a recorded session delivered: a projection naming a column the source has not.
		var job = new JobDefinition
		{
			Input = "generate:5",
			Output = "null:",
			Transformers = new List<TransformerConfig>
			{
				new() { Type = "project", Mappings = new Dictionary<string, string> { ["id"] = "" } }
			}
		};

		var exitCode = await pipelineService.ExecuteAsync(job, context: null, token: default);

		exitCode.Should().Be(1);
		pipelineService.Failure.Should().NotBeNull().And.Subject.Should().Contain("Projected column");
	}

	// Concrete fake exercising the real IsApplicable(JobDefinition) default interface method.
	private sealed class ThrowingStreamTransformerFactory : IStreamTransformerFactory
	{
		private readonly System.Exception _sentinel;
		public int CreateFromJobCalls { get; private set; }
		public int CliCreateCalls { get; private set; }

		public ThrowingStreamTransformerFactory(System.Exception sentinel) => _sentinel = sentinel;

		public string ComponentName => "merge";
		public string Category => "Stream Processors";
		public bool RequiresArrowChannels => true;
		public int MinStreams => 2;
		public int MaxStreams => -1;
		public int MinLookups => 0;
		public int MaxLookups => 0;
		public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => new[] { ("--merge", true) };

		public bool IsApplicable(string[] branchArgs) => false;

		public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider sp)
		{
			CliCreateCalls++;
			throw _sentinel;
		}

		// IsApplicable(JobDefinition) intentionally NOT overridden — the interface default runs.

		public IStreamTransformer CreateFromJob(JobDefinition job, BranchChannelContext ctx, IServiceProvider sp)
		{
			CreateFromJobCalls++;
			throw _sentinel;
		}
	}

	private static async IAsyncEnumerable<T> HelperAsyncEnumerable<T>(params T[] items)
	{
		await Task.Yield();
		foreach (var item in items)
		{
			yield return item;
		}
	}
}
