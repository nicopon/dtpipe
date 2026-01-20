using Spectre.Console;
using System.Diagnostics;
using QueryDump.Core;

namespace QueryDump.Feedback;

public sealed class ProgressReporter : IDisposable
{
    private readonly Stopwatch _stopwatch;
    private readonly bool _enabled;
    private bool _disposed;
    
    // Stats
    private long _readCount;
    private long _writeCount;
    private long _bytesWritten;

    // Transformers stats
    private readonly Dictionary<string, long> _transformerStats = new();
    private readonly List<string> _transformerNames = new();

    public ProgressReporter(bool enabled = true, IEnumerable<IDataTransformer>? transformers = null)
    {
        _enabled = enabled;
        _stopwatch = Stopwatch.StartNew();

        if (transformers != null)
        {
            foreach (var t in transformers)
            {
                _transformerNames.Add(t.GetType().Name);
                _transformerStats[t.GetType().Name] = 0;
            }
        }

        if (_enabled)
        {
            // Start the live display in a background task
            Task.Run(async () => 
            {
                await AnsiConsole.Live(CreateLayout())
                    .AutoClear(false)
                    .Overflow(VerticalOverflow.Ellipsis)
                    .Cropping(VerticalOverflowCropping.Bottom)
                    .StartAsync(async ctx => 
                    {
                        while (!_disposed)
                        {
                            ctx.UpdateTarget(CreateLayout());
                            await Task.Delay(100);
                        }
                    });
            });
        }
    }

    public void ReportRead(int count)
    {
        Interlocked.Add(ref _readCount, count);
        // Refresh is handled by background loop
    }

    public void ReportTransform(string transformerName, int count)
    {
        lock (_transformerStats)
        {
            if (_transformerStats.ContainsKey(transformerName))
            {
                _transformerStats[transformerName] += count;
            }
        }
    }

    public void ReportWrite(int count, long bytes)
    {
        Interlocked.Add(ref _writeCount, count);
        Interlocked.Add(ref _bytesWritten, bytes);
    }

    private Table CreateLayout()
    {
        var elapsed = _stopwatch.Elapsed.TotalSeconds;
        
        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Stage");
        table.AddColumn("Rows");
        table.AddColumn("Speed");

        // Reading
        var readSpeed = elapsed > 0 ? _readCount / elapsed : 0;
        table.AddRow("Reading", $"{_readCount:N0}", FormatSpeed(readSpeed));

        // Transformers
        lock (_transformerStats)
        {
            foreach (var name in _transformerNames)
            {
                var count = _transformerStats[name];
                var speed = elapsed > 0 ? count / elapsed : 0;
                table.AddRow($"→ {name}", $"{count:N0}", FormatSpeed(speed));
            }
        }

        // Writing
        var writeSpeed = elapsed > 0 ? _writeCount / elapsed : 0;
        table.AddRow("Writing", $"{_writeCount:N0}", FormatSpeed(writeSpeed));
        
        // Footer: File size
        table.AddRow("File Size", FormatBytes(Interlocked.Read(ref _bytesWritten)), "");

        return table;
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
            >= 1_000_000 => $"{rowsPerSec / 1_000_000:F1}M/s",
            >= 1_000 => $"{rowsPerSec / 1_000:F1}K/s",
            _ => $"{rowsPerSec:F0}/s"
        };
    }

    public void Complete()
    {
        _stopwatch.Stop();
        _disposed = true;
        if (_enabled)
        {
            // Give a small moment for the last refresh to happen if needed, or just let it close
            // We rely on the background task seeing _disposed = true and exiting the Live block
            
            AnsiConsole.MarkupLine($"[green]✓ Completed in {_stopwatch.Elapsed.TotalSeconds:F1}s | {_writeCount:N0} rows | {FormatBytes(_bytesWritten)}[/]");
        }
    }

    public void Dispose()
    {
        _disposed = true;
    }
}
