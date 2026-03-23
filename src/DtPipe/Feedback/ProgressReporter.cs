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
	private readonly bool _suppressLiveTui;
	private readonly string? _branchName;
	private bool _disposed;

	// Stats
	private long _readCount;
	private long _writeCount;
	private double _peakMemoryMb;
	private readonly DateTime _startTime;

	// Transformers stats
	private readonly Dictionary<string, long> _transformerStats = new();
	private readonly List<(string Name, bool IsColumnar)> _transformerModes = new();

	// UI Task
	private Task? _uiTask;

	public ProgressReporter(IAnsiConsole console, bool enabled = true, IReadOnlyList<(string Name, bool IsColumnar)>? transformerModes = null, bool suppressLiveTui = false, string? branchName = null)
	{
		_console = console;
		_enabled = enabled;
		_suppressLiveTui = suppressLiveTui;
		_branchName = branchName;
		_stopwatch = Stopwatch.StartNew();
		_startTime = DateTime.UtcNow;

		if (transformerModes != null)
		{
			foreach (var mode in transformerModes)
			{
				_transformerModes.Add(mode);
				_transformerStats[mode.Name] = 0;
			}
		}

		// Compute whether a live TUI should be started. Disable when output is
		// redirected, when stdout output is active, or in CI/non-interactive environments.
		_uiEnabled = _enabled && !_suppressLiveTui && !IsNonInteractiveEnvironment();

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
		// Normalize name to match the key registered at construction (strips "DataTransformer" suffix)
		var key = transformerName.Replace("DataTransformer", "");
		lock (_transformerStats)
		{
			if (_transformerStats.ContainsKey(key))
				_transformerStats[key] += count;
		}
	}

	public void ReportWrite(int count)
	{
		Interlocked.Add(ref _writeCount, count);
	}

	private Table CreateLayout()
	{
		var elapsed = _stopwatch.Elapsed.TotalSeconds;
		bool hasMode = _transformerModes.Count > 0;

		var table = new Table().Border(TableBorder.Rounded);
		if (_branchName != null)
			table.Title = new TableTitle($"[grey]{Markup.Escape(_branchName)}[/]");

		table.AddColumn(new TableColumn("Stage"));
		table.AddColumn(new TableColumn("Rows").RightAligned());
		table.AddColumn(new TableColumn("Speed").RightAligned());
		if (hasMode)
			table.AddColumn(new TableColumn("Mode"));

		// Reading
		var readSpeed = elapsed > 0 ? _readCount / elapsed : 0;
		if (hasMode)
			table.AddRow("[grey]▸ Reading[/]", $"[white]{_readCount:N0}[/]", $"[grey]{FormatSpeed(readSpeed)}[/]", "");
		else
			table.AddRow("[grey]▸ Reading[/]", $"[white]{_readCount:N0}[/]", $"[grey]{FormatSpeed(readSpeed)}[/]");

		// Transformers
		lock (_transformerStats)
		{
			foreach (var (name, isColumnar) in _transformerModes)
			{
				var count = _transformerStats[name];
				var speed = elapsed > 0 ? count / elapsed : 0;
				var modeLabel = isColumnar ? "[cyan]◈ columnar[/]" : "[yellow]● row[/]";
				table.AddRow($"[grey]▸ → {Markup.Escape(name)}[/]", $"[white]{count:N0}[/]", $"[grey]{FormatSpeed(speed)}[/]", modeLabel);
			}
		}

		// Writing
		var writeSpeed = elapsed > 0 ? _writeCount / elapsed : 0;
		if (hasMode)
			table.AddRow("[grey]▸ Writing[/]", $"[white]{_writeCount:N0}[/]", $"[grey]{FormatSpeed(writeSpeed)}[/]", "");
		else
			table.AddRow("[grey]▸ Writing[/]", $"[white]{_writeCount:N0}[/]", $"[grey]{FormatSpeed(writeSpeed)}[/]");

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

		UpdatePeakMemory();

		var completionLine = $"[green]✓[/] [white]{_writeCount:N0} rows[/] [grey]· {_stopwatch.Elapsed.TotalSeconds:F1}s · peak {_peakMemoryMb:F0} MB[/]";
		if (_uiEnabled)
		{
			_console.MarkupLine(completionLine);
		}
		else if (_enabled && _suppressLiveTui)
		{
			// TUI was suppressed (stdout output) but stats are enabled → print final summary to STDERR
			_console.Write(CreateLayout());
			_console.MarkupLine(completionLine);
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
				transformerStats[kvp.Key] = kvp.Value;
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
