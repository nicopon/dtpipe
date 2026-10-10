using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Feedback;

/// <summary>
/// Progress of a run nobody watches. It shows nothing but still counts reads and writes: the
/// sample report, the cursor's <c>RowsTransferred</c> and the completion log read those counts
/// whether or not a console is attached.
/// </summary>
internal sealed class NullExportProgress : IExportProgress
{
	private readonly DateTime _startTime = DateTime.UtcNow;
	private long _readCount;
	private long _writeCount;

	public void ReportRead(int count) => Interlocked.Add(ref _readCount, count);
	public void ReportTransform(string transformerName, int count) { }
	public void ReportWrite(int count) => Interlocked.Add(ref _writeCount, count);
	public void Complete() { }

	public ExportMetrics GetMetrics()
	{
		var now = DateTime.UtcNow;
		var written = Volatile.Read(ref _writeCount);
		var elapsed = (now - _startTime).TotalSeconds;
		return new ExportMetrics(
			_startTime, now, Volatile.Read(ref _readCount), written,
			elapsed > 0 ? written / elapsed : 0, 0, new Dictionary<string, long>());
	}

	public void Dispose() { }
}
