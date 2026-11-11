using DtPipe.Cli;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Feedback;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The results of a DAG whose branch failed list the branches that completed. A success line under them
/// would read as the run's verdict: the line is the run's own, whatever the table holds.
/// </summary>
public class DagRendererVerdictTests
{
    private static string Print(int exitCode)
    {
        var now = DateTime.UtcNow;
        var metrics = new ExportMetrics(now, now.AddSeconds(1), 5, 5, 5, 10, new Dictionary<string, long>());
        var results = new List<BranchSummary> { new("g", metrics, false, new List<(string, bool)>()) };
        var dag = new JobDagDefinition { Branches = new[] { new BranchDefinition { Alias = "g" } } };

        var writer = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Out = new AnsiConsoleOutput(writer),
        });
        DagRenderer.PrintUnifiedResultsTable(results, dag, isDag: true, console, exitCode);
        return writer.ToString();
    }

    [Fact]
    public void A_Completed_Run_Ends_On_The_Success_Line()
    {
        var text = Print(0);

        Assert.Contains("✓", text);
        Assert.DoesNotContain("✖", text);
    }

    [Theory]
    [InlineData(1, "Failed")]
    [InlineData(130, "Canceled")]
    public void A_Run_With_A_Non_Zero_Verdict_Shows_No_Success_Line(int exitCode, string word)
    {
        var text = Print(exitCode);

        Assert.DoesNotContain("✓", text);
        Assert.Contains("✖", text);
        Assert.Contains(word, text);
        Assert.Contains($"exit code {exitCode}", text);
        Assert.Contains("[g]", text);   // the rows are attributed to the branch that completed
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public void A_Linear_Run_Prints_Its_Summary_Only_When_It_Completed(bool completed, bool summary)
    {
        var writer = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Ansi = AnsiSupport.No,
            ColorSystem = ColorSystemSupport.NoColors,
            Out = new AnsiConsoleOutput(writer),
        });
        using var reporter = new ProgressReporter(console, enabled: true, suppressLiveTui: true);
        reporter.ReportWrite(7);

        if (completed) reporter.Complete(); else reporter.Abandon();

        Assert.Equal(summary, writer.ToString().Contains("7 rows"));
    }
}
