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

		_fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<(string, string)>>(v => v.Any(x => x.Item2 == "NAME:name.fullName"))))
			.Returns(fakeT1.Object);

		_nullFactory.Setup(f => f.CreateFromConfiguration(It.IsAny<IEnumerable<(string, string)>>()))
			.Returns(nullT.Object);

		_fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<(string, string)>>(v => v.Any(x => x.Item2 == "EMAIL:internet.email"))))
			.Returns(fakeT2.Object);

		_formatFactory.Setup(f => f.CreateFromConfiguration(It.IsAny<IEnumerable<(string, string)>>()))
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
		var fakeGroup2 = new Mock<IDataTransformer>();
		var nullGroup = new Mock<IDataTransformer>();

		_fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<(string, string)>>(v => v.Count() == 2 && v.Any(x => x.Item2 == "A:a") && v.Any(x => x.Item2 == "B:b"))))
			.Returns(fakeGroup1.Object);

		_nullFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<(string, string)>>(v => v.Any(x => x.Item2 == "C"))))
			.Returns(nullGroup.Object);

		_fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<(string, string)>>(v => v.Count() == 1 && v.Any(x => x.Item2 == "D:d"))))
			.Returns(fakeGroup2.Object);

		// Act
		var pipeline = builder.Build(args);

		// Assert
		pipeline.Should().HaveCount(3);
		pipeline[0].Should().Be(fakeGroup1.Object); // Fake [A, B]
		pipeline[1].Should().Be(nullGroup.Object);  // Null [C]
		pipeline[2].Should().Be(fakeGroup2.Object); // Fake [D]
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

		// Expectation: CreateFromConfiguration called with SkipNull=true and Fake=Value in the same group (same factory)
		_fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<(string, string)>>(v =>
			v.Any(x => x.Item1 == "--skip-null" && x.Item2 == "true") &&
			v.Any(x => x.Item1 == "--fake" && x.Item2 == "Value"))))
			.Returns(fakeT.Object);

		// Act
		var pipeline = builder.Build(args);

		// Assert
		pipeline.Should().HaveCount(1);
		pipeline[0].Should().Be(fakeT.Object);
	}
}
