using System;
using System.IO;
using DtPipe.Feedback;
using Xunit;
using Moq;
using Spectre.Console;

namespace DtPipe.Tests.Unit.Feedback;

public class ProgressReporterTests
{
    [Fact]
    public void Complete_DoesNotWrite_Markup_When_DtPipeNoTui_IsSet()
    {
        var prev = Environment.GetEnvironmentVariable("DTPIPE_NO_TUI");
        try
        {
            Environment.SetEnvironmentVariable("DTPIPE_NO_TUI", "1");

            var sw = new StringWriter();
            var prevOut = Console.Out;
            Console.SetOut(sw);

            try
            {
                using var reporter = new ProgressReporter(new Mock<IAnsiConsole>().Object, enabled: true, transformers: null);
                // perform some activity reports
                reporter.ReportRead(1);
                reporter.ReportWrite(1);
                reporter.Complete();
            }
            finally
            {
                Console.SetOut(prevOut);
            }

            var output = sw.ToString();
            Assert.DoesNotContain("✓ Completed", output);
            Assert.DoesNotContain("Completed in", output);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DTPIPE_NO_TUI", prev);
        }
    }
}
