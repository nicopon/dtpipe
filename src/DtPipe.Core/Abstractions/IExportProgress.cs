namespace DtPipe.Core.Abstractions;

public interface IExportProgress : IDisposable
{
    void ReportRead(int count);
    /// <summary>
    /// Counts the rows a transformer let through. <paramref name="stage"/> is its place in the pipeline,
    /// counted from 1 (stage 0 is the reader), the numbering <c>ISampleTap</c> uses: two transformers of one
    /// type are two stages, whatever they are called.
    /// </summary>
    void ReportTransform(int stage, int count);
    void ReportWrite(int count);
    void Complete();
    DtPipe.Core.Models.ExportMetrics GetMetrics();
}
