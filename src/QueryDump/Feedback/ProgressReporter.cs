using System.Diagnostics;

namespace QueryDump.Feedback;

/// <summary>
/// Reports progress to STDERR with 500ms refresh, overwriting the same line.
/// </summary>
public sealed class ProgressReporter : IDisposable
{
    private readonly Timer _timer;
    private readonly Stopwatch _stopwatch;
    private readonly object _lock = new();
    
    private long _rowCount;
    private long _bytesWritten;
    private bool _disposed;
    private int _lastLineLength;

    public ProgressReporter()
    {
        _stopwatch = Stopwatch.StartNew();
        _timer = new Timer(OnTick, null, TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(500));
    }

    public void Update(long rowCount, long bytesWritten)
    {
        lock (_lock)
        {
            _rowCount = rowCount;
            _bytesWritten = bytesWritten;
        }
    }

    private void OnTick(object? state)
    {
        if (_disposed) return;

        long rows, bytes;
        lock (_lock)
        {
            rows = _rowCount;
            bytes = _bytesWritten;
        }

        var elapsed = _stopwatch.Elapsed;
        var rowsPerSec = elapsed.TotalSeconds > 0 ? rows / elapsed.TotalSeconds : 0;
        
        var message = $"\rRows: {rows:N0} | File: {FormatBytes(bytes)} | Speed: {FormatSpeed(rowsPerSec)}";
        
        // Pad with spaces to overwrite previous content
        var padding = Math.Max(0, _lastLineLength - message.Length);
        var output = message + new string(' ', padding);
        
        Console.Error.Write(output);
        _lastLineLength = message.Length;
    }

    private static string FormatBytes(long bytes)
    {
        return bytes switch
        {
            >= 1_073_741_824 => $"{bytes / 1_073_741_824.0:F1} GB",
            >= 1_048_576 => $"{bytes / 1_048_576.0:F1} MB",
            >= 1024 => $"{bytes / 1024.0:F1} KB",
            _ => $"{bytes} B"
        };
    }

    private static string FormatSpeed(double rowsPerSec)
    {
        return rowsPerSec switch
        {
            >= 1_000_000 => $"{rowsPerSec / 1_000_000:F1}M rows/s",
            >= 1_000 => $"{rowsPerSec / 1_000:F1}K rows/s",
            _ => $"{rowsPerSec:F0} rows/s"
        };
    }

    public void Complete()
    {
        // Final update
        OnTick(null);
        Console.Error.WriteLine(); // New line after completion
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _timer.Dispose();
        _stopwatch.Stop();
    }
}
