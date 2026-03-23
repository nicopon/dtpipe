using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Feedback;

internal sealed class NullExportProgress : IExportProgress
{
	public void ReportRead(int count) { }
	public void ReportTransform(string transformerName, int count) { }
	public void ReportWrite(int count) { }
	public void Complete() { }
	public ExportMetrics GetMetrics() => new(DateTime.UtcNow, DateTime.UtcNow, 0, 0, 0, 0, new Dictionary<string, long>());
	public void Dispose() { }
}
