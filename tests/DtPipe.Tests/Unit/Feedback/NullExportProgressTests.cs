using DtPipe.Feedback;
using Xunit;

namespace DtPipe.Tests.Unit.Feedback;

public class NullExportProgressTests
{
	[Fact]
	public void Counts_Reads_And_Writes_Without_Showing_Anything()
	{
		using var progress = new NullExportProgress();

		progress.ReportRead(5);
		progress.ReportRead(3);
		progress.ReportWrite(6);

		var metrics = progress.GetMetrics();
		Assert.Equal(8, metrics.ReadCount);
		Assert.Equal(6, metrics.WriteCount);
	}
}
