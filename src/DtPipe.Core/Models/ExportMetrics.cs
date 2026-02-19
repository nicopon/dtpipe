namespace DtPipe.Core.Models;

/// <summary>
/// Structured report of an export job execution.
/// </summary>
public sealed record ExportMetrics(
    DateTime StartTime,
    DateTime EndTime,
    long ReadCount,
    long WriteCount,
    double OverallThroughputRowsPerSec,
    double PeakMemoryWorkingSetMb,
    IReadOnlyDictionary<string, long> TransformerStats
)
{
    public TimeSpan Duration => EndTime - StartTime;
}
