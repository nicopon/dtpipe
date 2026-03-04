using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Services;
using Moq;
using Xunit;

namespace DtPipe.Tests.Unit.Services;

public class PipelineSegmenterTests
{
    [Fact]
    public void GetSegments_EmptyPipeline_ReturnsEmptyList()
    {
        // Act
        var segments = PipelineSegmenter.GetSegments(new List<IDataTransformer>());

        // Assert
        Assert.Empty(segments);
    }

    [Fact]
    public void GetSegments_OnlyRowBased_ReturnsSingleRowSegment()
    {
        // Arrange
        var mockRowT = new Mock<IDataTransformer>();
        var pipeline = new List<IDataTransformer> { mockRowT.Object };

        // Act
        var segments = PipelineSegmenter.GetSegments(pipeline);

        // Assert
        Assert.Single(segments);
        Assert.False(segments[0].IsColumnar);
        Assert.Equal(pipeline, segments[0].Transformers);
    }

    [Fact]
    public void GetSegments_OnlyColumnar_ReturnsSingleColumnarSegment()
    {
        // Arrange
        var mockColT = new Mock<IColumnarTransformer>();
        mockColT.As<IDataTransformer>();
        mockColT.Setup(t => t.CanProcessColumnar).Returns(true);
        var pipeline = new List<IDataTransformer> { mockColT.Object };

        // Act
        var segments = PipelineSegmenter.GetSegments(pipeline);

        // Assert
        Assert.Single(segments);
        Assert.True(segments[0].IsColumnar);
        Assert.Equal(pipeline, segments[0].Transformers);
    }

    [Fact]
    public void GetSegments_MixedTypes_ReturnsCorrectAlternatingSegments()
    {
        // Arrange
        var mockCol1 = CreateMockColumnar(true);
        var mockRow1 = new Mock<IDataTransformer>().Object;
        var mockCol2 = CreateMockColumnar(true);

        var pipeline = new List<IDataTransformer> { mockCol1, mockRow1, mockCol2 };

        // Act
        var segments = PipelineSegmenter.GetSegments(pipeline);

        // Assert
        Assert.Equal(3, segments.Count);
        Assert.True(segments[0].IsColumnar);
        Assert.False(segments[1].IsColumnar);
        Assert.True(segments[2].IsColumnar);

        Assert.Single(segments[0].Transformers);
        Assert.Equal(mockCol1, segments[0].Transformers[0]);

        Assert.Single(segments[1].Transformers);
        Assert.Equal(mockRow1, segments[1].Transformers[0]);

        Assert.Single(segments[2].Transformers);
        Assert.Equal(mockCol2, segments[2].Transformers[0]);
    }

    [Fact]
    public void GetSegments_GroupSameTypes_IntoOneSegment()
    {
        // Arrange
        var mockCol1 = CreateMockColumnar(true);
        var mockCol2 = CreateMockColumnar(true);
        var mockRow1 = new Mock<IDataTransformer>().Object;

        var pipeline = new List<IDataTransformer> { mockCol1, mockCol2, mockRow1 };

        // Act
        var segments = PipelineSegmenter.GetSegments(pipeline);

        // Assert
        Assert.Equal(2, segments.Count);
        Assert.True(segments[0].IsColumnar);
        Assert.Equal(2, segments[0].Transformers.Count);
        Assert.False(segments[1].IsColumnar);
        Assert.Single(segments[1].Transformers);
    }

    [Fact]
    public void GetSegments_WhenColumnarTransformerExplicitlyRefuses_TreatedAsRow()
    {
        // Arrange
        var mockColRefuses = CreateMockColumnar(false);
        var pipeline = new List<IDataTransformer> { mockColRefuses };

        // Act
        var segments = PipelineSegmenter.GetSegments(pipeline);

        // Assert
        Assert.Single(segments);
        Assert.False(segments[0].IsColumnar);
    }

    private static IDataTransformer CreateMockColumnar(bool canProcess)
    {
        var mock = new Mock<IColumnarTransformer>();
        mock.As<IDataTransformer>();
        mock.Setup(t => t.CanProcessColumnar).Returns(canProcess);
        return mock.Object;
    }
}
