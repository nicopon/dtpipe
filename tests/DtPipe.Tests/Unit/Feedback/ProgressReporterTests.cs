using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Engine;
using DtPipe.Feedback;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Spectre.Console;
using Xunit;

namespace DtPipe.Tests.Unit.Feedback;

[Collection("console-serial")]
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
                using var reporter = new ProgressReporter(new Mock<IAnsiConsole>().Object, enabled: true, transformerModes: null);
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

    [Fact]
    public void GetMetrics_ReturnsCorrectData()
    {
        var console = new Mock<IAnsiConsole>();
        var reporter = new ProgressReporter(console.Object, enabled: true, transformerModes: new[] { (Name: "TestTr", IsColumnar: false) });

        reporter.ReportRead(10);
        reporter.ReportTransform(1, 5);
        reporter.ReportWrite(8);

        var metrics = reporter.GetMetrics();

        Assert.Equal(10, metrics.ReadCount);
        Assert.Equal(8, metrics.WriteCount);
        Assert.True(metrics.TransformerStats.ContainsKey("TestTr"));
        Assert.Equal(5, metrics.TransformerStats["TestTr"]);
        Assert.True(metrics.PeakMemoryWorkingSetMb > 0);
    }

    private sealed class KeepDataTransformer(Func<int, bool> keep) : IDataTransformer
    {
        public ValueTask<IReadOnlyList<PipeColumnInfo>> InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
            => new(columns);

        public object?[]? Transform(IReadOnlyList<object?> row) => keep((int)row[0]!) ? row.ToArray() : null;
    }

    [Fact]
    public void Two_Transformers_Of_One_Type_Count_Their_Own_Rows_When_Both_Drop_Some()
    {
        var executor = new PipelineExecutor(
            Enumerable.Empty<IRowToColumnarBridgeFactory>(),
            Enumerable.Empty<IColumnarToRowBridgeFactory>(),
            NullLogger<PipelineExecutor>.Instance);
        var transformers = new List<IDataTransformer>
        {
            new KeepDataTransformer(v => v % 3 != 0),
            new KeepDataTransformer(v => v % 2 == 0)
        };
        var modes = transformers
            .Select(t => (Name: t.GetType().Name.Replace("DataTransformer", ""), IsColumnar: false))
            .ToArray();
        using var reporter = new ProgressReporter(new Mock<IAnsiConsole>().Object, enabled: false, transformerModes: modes);

        for (var v = 0; v < 10; v++)
            executor.ProcessRowThroughTransformers(new object?[] { v }, transformers, reporter, default);

        // Rows 1,2,4,5,7,8 leave the first stage; of those 2,4,8 leave the second.
        Assert.Equal(new long[] { 6, 3 }, reporter.TransformerCountsByIndex);
    }
}
