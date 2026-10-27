using DtPipe.Observers;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Observers;

public class SpectreConsoleObserverTests
{
    /// <summary>
    /// One column and a one-digit sample count make the smallest panel there is, and its body is still
    /// narrower than its header: the panel widens to the whole header instead of cutting it with an ellipsis.
    /// </summary>
    [Fact]
    public void AutoAppliedPanel_ShowsItsWholeHeader_OverAShorterBody()
    {
        var writer = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Out = new AnsiConsoleOutput(writer)
        });
        console.Profile.Width = 400;

        new SpectreConsoleObserver(console).ShowColumnTypeInferenceSuggestion(
            new Dictionary<string, string> { ["id"] = "int32" }, sampleCount: 1, applied: true);

        Assert.Contains("Column types auto-applied (sampled 1 rows)", writer.ToString());
    }
}
