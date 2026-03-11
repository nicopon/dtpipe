using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Services;

/// <summary>
/// A segment of the pipeline that is either entirely columnar or entirely row-based.
/// </summary>
internal sealed class PipelineSegment
{
    public bool IsColumnar { get; }
    public List<IDataTransformer> Transformers { get; }
    public IReadOnlyList<PipeColumnInfo> InputSchema { get; set; } = Array.Empty<PipeColumnInfo>();
    public IReadOnlyList<PipeColumnInfo> OutputSchema { get; set; } = Array.Empty<PipeColumnInfo>();

    public PipelineSegment(bool isColumnar, List<IDataTransformer> transformers)
    {
        IsColumnar = isColumnar;
        Transformers = transformers;
    }
}

/// <summary>
/// Static utility to segment a pipeline into homogeneous columnar or row-based segments.
/// </summary>
internal static class PipelineSegmenter
{
    public static List<PipelineSegment> GetSegments(List<IDataTransformer> pipeline)
    {
        var segments = new List<PipelineSegment>();
        if (pipeline.Count == 0) return segments;

        PipelineSegment? current = null;
        foreach (var t in pipeline)
        {
            bool isCol = t is IColumnarTransformer ct && ct.CanProcessColumnar;
            if (current == null || current.IsColumnar != isCol)
            {
                current = new PipelineSegment(isCol, new List<IDataTransformer>());
                segments.Add(current);
            }
            current.Transformers.Add(t);
        }
        return segments;
    }
}
