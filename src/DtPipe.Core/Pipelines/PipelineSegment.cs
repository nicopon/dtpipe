using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Core.Pipelines;

/// <summary>
/// A contiguous segment of the pipeline that is either entirely columnar or entirely row-based.
/// Segments are produced by <see cref="PipelineSegmenter"/> and consumed by the pipeline executor.
/// </summary>
public sealed class PipelineSegment
{
    public bool IsColumnar { get; }
    public List<IDataTransformer> Transformers { get; }
    public IReadOnlyList<PipeColumnInfo> InputSchema { get; set; } = Array.Empty<PipeColumnInfo>();
    public Apache.Arrow.Schema? InputSchemaArrow { get; set; }
    public IReadOnlyList<PipeColumnInfo> OutputSchema { get; set; } = Array.Empty<PipeColumnInfo>();

    public PipelineSegment(bool isColumnar, List<IDataTransformer> transformers)
    {
        IsColumnar = isColumnar;
        Transformers = transformers;
    }
}

/// <summary>
/// Segments a transformer pipeline into homogeneous columnar or row-based runs.
/// Consecutive columnar-capable transformers are grouped into a single columnar segment,
/// enabling Arrow zero-copy fast-path between them.
/// </summary>
public static class PipelineSegmenter
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
