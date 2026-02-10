namespace DtPipe.Cli.DryRun;

using DtPipe.DryRun;
using Spectre.Console;

/// <summary>
/// Interactive keyboard navigator for dry-run samples.
/// </summary>
public class DryRunNavigator
{
	private readonly DryRunRenderer _renderer;
	private readonly IAnsiConsole _console;

	public DryRunNavigator(DryRunRenderer renderer, IAnsiConsole console)
	{
		_renderer = renderer;
		_console = console;
	}

	/// <summary>
	/// Starts interactive navigation through samples.
	/// Returns the index of the sample that was displayed when the user exited.
	/// </summary>
	/// <param name="samples">Sample traces to navigate</param>
	/// <param name="stepNames">Pipeline step names</param>
	/// <param name="columnWidths">Fixed column widths for stable layout</param>
	/// <param name="schemaWarning">Warning message if schema inspection failed (shown in Output column header)</param>
	public int Navigate(
		List<SampleTrace> samples,
		List<string> stepNames,
		int[]? columnWidths = null,
		string? schemaWarning = null,
		Core.Models.TargetSchemaInfo? targetSchema = null,
		int startIndex = 0,
		List<int>? errorIndices = null)
	{
		if (samples.Count == 0) return 0;

		int currentIndex = Math.Clamp(startIndex, 0, samples.Count - 1);
		errorIndices ??= new List<int>();

		while (true)
		{
			Console.Clear();

			var table = _renderer.BuildTraceTable(currentIndex, samples.Count, samples[currentIndex], stepNames, columnWidths, schemaWarning, targetSchema);
			_console.Write(table);
			_console.WriteLine();

			// Single sample: exit immediately without navigation
			if (samples.Count == 1)
			{
				_console.MarkupLine("[green]Dry-run complete. No data exported.[/]");
				return 0;
			}

			_console.MarkupLine("[dim]← → Navigate | ↑ Next Error | ↓ Prev Error | Enter/Esc Exit[/]");

			var key = Console.ReadKey(true);

			switch (key.Key)
			{
				case ConsoleKey.LeftArrow:
					currentIndex = Math.Max(0, currentIndex - 1);
					break;
				case ConsoleKey.RightArrow:
					currentIndex = Math.Min(samples.Count - 1, currentIndex + 1);
					break;
				case ConsoleKey.DownArrow: // Jump to previous error (inverted based on user request)
					var prevError = errorIndices.Where(i => i < currentIndex).OrderByDescending(i => i).FirstOrDefault(-1);
					if (prevError != -1) currentIndex = prevError;
					break;
				case ConsoleKey.UpArrow: // Jump to next error (inverted based on user request)
					var nextError = errorIndices.Where(i => i > currentIndex).OrderBy(i => i).FirstOrDefault(-1);
					if (nextError != -1) currentIndex = nextError;
					break;
				case ConsoleKey.Enter:
				case ConsoleKey.Escape:
					// Re-render final table without navigation hint
					Console.Clear();
					var finalTable = _renderer.BuildTraceTable(currentIndex, samples.Count, samples[currentIndex], stepNames, columnWidths, schemaWarning, targetSchema);
					_console.Write(finalTable);
					_console.WriteLine();
					_console.MarkupLine("[green]Dry-run complete. No data exported.[/]");
					return currentIndex;
			}
		}
	}
}
