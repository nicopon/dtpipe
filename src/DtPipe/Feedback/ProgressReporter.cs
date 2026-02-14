using System.Diagnostics;
using DtPipe.Core.Abstractions;
using Spectre.Console;

namespace DtPipe.Feedback;

public sealed class ProgressReporter : IExportProgress
{
	private readonly Stopwatch _stopwatch;
	private readonly IAnsiConsole _console;
	private readonly bool _enabled;
	private readonly bool _uiEnabled;
	private bool _disposed;

	// Stats
	private long _readCount;
	private long _writeCount;
	private double _peakMemoryMb;
	private readonly DateTime _startTime;

	// Transformers stats
	private readonly Dictionary<string, long> _transformerStats = new();
	private readonly List<string> _transformerNames = new();

	// UI Task
	private Task? _uiTask;

	public ProgressReporter(IAnsiConsole console, bool enabled = true, IEnumerable<string>? transformerNames = null)
	{
		_console = console;
		_enabled = enabled;
		_stopwatch = Stopwatch.StartNew();
		_startTime = DateTime.UtcNow;

		if (transformerNames != null)
		{
			foreach (var name in transformerNames)
			{
				_transformerNames.Add(name);
				_transformerStats[name] = 0;
			}
		}

		// Compute whether a live TUI should be started. Disable when output is
		// redirected or when a CI/non-interactive environment is detected.
		_uiEnabled = _enabled && !IsNonInteractiveEnvironment();

		if (_uiEnabled)
		{
			// Start the live display in a background task but make it tolerant to
			// non-interactive/test environments where Spectre.Console may throw
			// when multiple interactive displays are attempted concurrently.
			_uiTask = Task.Run(async () =>
			{
				try
				{
					await _console.Live(CreateLayout())
						.AutoClear(false)
						.Overflow(VerticalOverflow.Ellipsis)
						.Cropping(VerticalOverflowCropping.Bottom)
						.StartAsync(async ctx =>
						{
							while (!_disposed)
							{
								UpdatePeakMemory();
								ctx.UpdateTarget(CreateLayout());
								try { await Task.Delay(500); } catch (TaskCanceledException) { break; }
							}
							// Ensure final update
							ctx.UpdateTarget(CreateLayout());
						});
				}
				catch (InvalidOperationException)
				{
					// Spectre.Console can throw when interactive displays are used
					// concurrently (e.g. during tests). Silently ignore and continue
					// without a live UI.
				}
			}).ContinueWith(t => { /* swallow exceptions from the UI task */ });
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

	public void ReportWrite(int count)
	{
		Interlocked.Add(ref _writeCount, count);
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

		return table;
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

		if (_uiTask != null)
		{
			try { _uiTask.Wait(1000); } catch { /* ignore UI task failures/timeouts */ }
		}

		if (_uiEnabled)
		{
			_console.MarkupLine($"[green]✓ Completed in {_stopwatch.Elapsed.TotalSeconds:F1}s | {_writeCount:N0} rows[/]");
		}
	}

	private bool IsNonInteractiveEnvironment()
	{
		// Explicit opt-out
		var noTui = Environment.GetEnvironmentVariable("DTPIPE_NO_TUI");
		if (!string.IsNullOrWhiteSpace(noTui) && (noTui == "1" || noTui.Equals("true", StringComparison.OrdinalIgnoreCase)))
		{
			return true;
		}

		// If STDOUT is redirected (e.g. piped), we should disable the interactive TUI
		// to avoid polluting the terminal or confusing the user, even if TUI goes to STDERR.
		if (Console.IsOutputRedirected) return true;

		// Trust Spectre Console detection if configured
		if (!_console.Profile.Capabilities.Interactive) return true;

		// Common CI indicators
		if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("CI"))) return true;

		return false;
	}

	public DtPipe.Core.Models.ExportMetrics GetMetrics()
	{
		UpdatePeakMemory();

		var transformerStats = new Dictionary<string, long>();
		lock (_transformerStats)
		{
			foreach (var kvp in _transformerStats)
			{
				transformerStats[kvp.Key] = kvp.Value;
			}
		}

		var elapsed = _stopwatch.Elapsed.TotalSeconds;
		var overallThroughput = elapsed > 0 ? _writeCount / elapsed : 0;

		return new DtPipe.Core.Models.ExportMetrics(
			_startTime,
			DateTime.UtcNow,
			_readCount,
			_writeCount,
			overallThroughput,
			_peakMemoryMb,
			transformerStats
		);
	}

	private void UpdatePeakMemory()
	{
		try
		{
			using var process = Process.GetCurrentProcess();
			var currentMemory = process.WorkingSet64 / 1024.0 / 1024.0;
			if (currentMemory > _peakMemoryMb)
			{
				_peakMemoryMb = currentMemory;
			}
		}
		catch
		{
			// Ignore if process info cannot be accessed
		}
	}

	public void Dispose()
	{
		_disposed = true;
		if (_uiTask != null && !_uiTask.IsCompleted)
		{
			try { _uiTask.Wait(500); } catch { }
		}
	}
}
