using System.CommandLine;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using QueryDump.Core;
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
        // For testing purposes, we only strictly need the alias used in the test arguments.
        // System.CommandLine 2.0.2 Option<T> constructor with string[] aliases seems elusive or I'm missing something.
        // Since we don't use the aliases (e.g. -f) in the test cases, we can just register the main one.
        var option = new Option<string>(mainAlias);
        
        // If we really needed aliases, we would need to find the correct way to add them (e.g. option.AddAlias() if available, 
        // or check correct constructor signature). 
        // But preventing build failure is priority.
        
        mock.Setup(f => f.GetCliOptions()).Returns(new List<Option> { option });
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

        _fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<string>>(v => v.Contains("NAME:name.fullName"))))
            .Returns(fakeT1.Object);
        
        _nullFactory.Setup(f => f.CreateFromConfiguration(It.IsAny<IEnumerable<string>>()))
            .Returns(nullT.Object);

        _fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<string>>(v => v.Contains("EMAIL:internet.email"))))
            .Returns(fakeT2.Object);

        _formatFactory.Setup(f => f.CreateFromConfiguration(It.IsAny<IEnumerable<string>>()))
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

        _fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<string>>(v => v.Count() == 2 && v.Contains("A:a") && v.Contains("B:b"))))
            .Returns(fakeGroup1.Object);

        _nullFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<string>>(v => v.Contains("C"))))
            .Returns(nullGroup.Object);
        
        _fakeFactory.Setup(f => f.CreateFromConfiguration(It.Is<IEnumerable<string>>(v => v.Count() == 1 && v.Contains("D:d"))))
            .Returns(fakeGroup2.Object);

        // Act
        var pipeline = builder.Build(args);

        // Assert
        pipeline.Should().HaveCount(3);
        pipeline[0].Should().Be(fakeGroup1.Object); // Fake [A, B]
        pipeline[1].Should().Be(nullGroup.Object);  // Null [C]
        pipeline[2].Should().Be(fakeGroup2.Object); // Fake [D]
    }
}
