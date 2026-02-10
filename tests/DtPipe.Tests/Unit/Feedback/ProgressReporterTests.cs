using DtPipe.Feedback;
using Moq;
using Spectre.Console;
using Xunit;

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
				using var reporter = new ProgressReporter(new Mock<IAnsiConsole>().Object, enabled: true, transformerNames: null);
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
