using QueryDump.Core.Pipelines;
using System.CommandLine;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using QueryDump.Core.Abstractions;
using QueryDump.Core.Models;
using QueryDump.Cli.Abstractions;
using QueryDump.Core.Options;
using QueryDump.Transformers.Fake;
using QueryDump.Transformers.Format;
using QueryDump.Transformers.Null;
using QueryDump.Transformers.Overwrite;
using Xunit;

namespace QueryDump.Tests;

public class OrderedPipelineTests
{
    private readonly Mock<IFakeDataTransformerFactory> _fakeFactory;
    private readonly Mock<INullDataTransformerFactory> _nullFactory;
    private readonly Mock<IFormatDataTransformerFactory> _formatFactory;
    private readonly Mock<IOverwriteDataTransformerFactory> _staticFactory;
    private readonly List<IDataTransformerFactory> _factories;

    public OrderedPipelineTests()
    {
        _fakeFactory = new Mock<IFakeDataTransformerFactory>();
        SetupFactory(_fakeFactory, "--fake", "-f");

        _nullFactory = new Mock<INullDataTransformerFactory>();
        SetupFactory(_nullFactory, "--null");

        _formatFactory = new Mock<IFormatDataTransformerFactory>();
        SetupFactory(_formatFactory, "--format");
        
        _staticFactory = new Mock<IOverwriteDataTransformerFactory>();
        SetupFactory(_staticFactory, "--overwrite");

        _factories = new List<IDataTransformerFactory> 
        { 
            _fakeFactory.Object, 
            _nullFactory.Object, 
            _formatFactory.Object,
            _staticFactory.Object 
        };
    }

    private void SetupFactory<T>(Mock<T> mock, string mainAlias, params string[] aliases) where T : class, IDataTransformerFactory
    {
        var option = new Option<string>(mainAlias);
        
        // Since IDataTransformerFactory no longer inherits ICliContributor (Refactoring Prop 1),
        // we need to mock the intersection IDataTransformerFactory + ICliContributor.
        // Moq's As<TInterface>() allows us to add an interface implementation to the mock.
        
        mock.As<ICliContributor>().Setup(f => f.GetCliOptions()).Returns(new List<Option> { option });
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

        // We expect:
        // 1. Fake (NAME)
        // 2. Null (Observed)
        // 3. Fake (EMAIL)
        // 4. Format (DISPLAY)

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
        pipeline[2].Should().Be(fakeGroup2.Object); // Fake [D]
    }

    [Fact]
    public void Build_ShouldHandleFlags_WithoutConsumingNextToken()
    {
        // Arrange
        var builder = new TransformerPipelineBuilder(_factories);
        
        // Setup --skip-null as a FLAG (Arity 0) for Fake factory
        // NOTE: We override the global setup for this test
        var skipNullOption = new Option<bool>("--skip-null") { Arity = ArgumentArity.Zero };
        var fakeOption = new Option<string>("--fake");
        
        _fakeFactory.As<ICliContributor>().Setup(f => f.GetCliOptions()).Returns(new List<Option> { fakeOption, skipNullOption });

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
