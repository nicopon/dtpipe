namespace QueryDump.DryRun;

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
    public int Navigate(List<SampleTrace> samples, List<string> stepNames, int[]? columnWidths = null)
    {
        if (samples.Count == 0) return 0;

        int currentIndex = 0;
        
        while (true)
        {
            Console.Clear();
            
            var table = _renderer.BuildTraceTable(currentIndex, samples.Count, samples[currentIndex], stepNames, columnWidths);
            _console.Write(table);
            _console.WriteLine();
            
            // Single sample: exit immediately without navigation
            if (samples.Count == 1)
            {
                _console.MarkupLine("[green]Dry-run complete. No data exported.[/]");
                return 0;
            }
            
            _console.MarkupLine("[dim]← → Navigate samples | Enter/Esc Exit[/]");
            
            var key = Console.ReadKey(true);
            
            switch (key.Key)
            {
                case ConsoleKey.LeftArrow:
                    currentIndex = Math.Max(0, currentIndex - 1);
                    break;
                case ConsoleKey.RightArrow:
                    currentIndex = Math.Min(samples.Count - 1, currentIndex + 1);
                    break;
                case ConsoleKey.Enter:
                case ConsoleKey.Escape:
                    // Re-render final table without navigation hint
                    Console.Clear();
                    var finalTable = _renderer.BuildTraceTable(currentIndex, samples.Count, samples[currentIndex], stepNames, columnWidths);
                    _console.Write(finalTable);
                    _console.WriteLine();
                    _console.MarkupLine("[green]Dry-run complete. No data exported.[/]");
                    return currentIndex;
            }
        }
    }
}
