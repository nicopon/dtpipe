using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace DtPipe.Tests;

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
		var skipNullFlag = new FlagDef("--skip-null", Array.Empty<string>(), FlagArity.Boolean, FlagScope.PerBranch, "fake");
		var fakeFlag = new FlagDef("--fake", Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "fake");

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
}
