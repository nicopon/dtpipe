namespace DtPipe.Core.Abstractions;

public interface IExportProgress : IDisposable
{
	void ReportRead(int count);
	void ReportTransform(string transformerName, int count);
	void ReportWrite(int count);
	void Complete();
	DtPipe.Core.Models.ExportMetrics GetMetrics();
}
