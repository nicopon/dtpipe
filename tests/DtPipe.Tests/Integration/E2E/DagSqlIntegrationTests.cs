using System;
using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests.Integration.E2E;

public class DagSqlIntegrationTests : IAsyncLifetime
{
    private readonly List<string> _cleanupPaths = new();

    private string GetTempPath()
    {
        var path = Path.Combine(Path.GetTempPath(), $"dag_sql_test_{Guid.NewGuid()}.csv");
        _cleanupPaths.Add(path);
        return path;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        foreach (var path in _cleanupPaths)
        {
            try
            {
                if (File.Exists(path)) File.Delete(path);
            }
            catch { /* ignore */ }
        }
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task DagPipeline_ShouldCorrectlyFilterData_UsingDuckDBSqlProcessor()
    {
        var outputPath = GetTempPath();
        var args = new[]
        {
            "--input", "generate:10", "--alias", "test",
            "--from", "test", 
            "select GenerateIndex from test where GenerateIndex < 5", 
            "--output", $"csv:{outputPath}"
        };

        var exitCode = await DtPipe.Program.Main(args);
        
        exitCode.Should().Be(0, "the pipeline should execute successfully");
        File.Exists(outputPath).Should().BeTrue("the output CSV file should be created");

        var lines = await File.ReadAllLinesAsync(outputPath);
        lines.Should().HaveCount(6, "the SQL WHERE clause should have filtered the 10 input rows down to 5");

        lines[0].Should().Be("GenerateIndex");
        lines[1].Should().Be("0");
        lines[5].Should().Be("4");
    }
    [Fact]
    public async Task DagPipeline_ShouldExecuteFanOut_WithMultipleSqlBranches()
    {
        var out1 = GetTempPath();
        var out2 = GetTempPath();

        var args = new[]
        {
            "--input", "generate:10", "--alias", "src",
            "--from", "src", "select GenerateIndex from src where GenerateIndex < 5", "--output", $"csv:{out1}",
            "--from", "src", "select GenerateIndex from src where GenerateIndex >= 5", "--output", $"csv:{out2}"
        };

        var exitCode = await DtPipe.Program.Main(args);
        exitCode.Should().Be(0);

        var lines1 = await File.ReadAllLinesAsync(out1);
        lines1.Should().HaveCount(6);
        lines1[1].Should().Be("0");
        lines1[5].Should().Be("4");

        var lines2 = await File.ReadAllLinesAsync(out2);
        lines2.Should().HaveCount(6);
        lines2[1].Should().Be("5");
        lines2[5].Should().Be("9");
    }

    [Fact]
    public async Task DagPipeline_ShouldExecuteMerge_FromMultipleSources()
    {
        var outputPath = GetTempPath();

        var args = new[]
        {
            "--input", "generate:10", "--alias", "b1",
            "--input", "generate:5", "--alias", "b2",
            "--from", "b1,b2", "--merge", "--output", $"csv:{outputPath}"
        };

        var exitCode = await DtPipe.Program.Main(args);
        exitCode.Should().Be(0);

        var lines = await File.ReadAllLinesAsync(outputPath);
        lines.Should().HaveCount(16, "10 from b1 + 5 from b2 + 1 header");
    }

    [Fact]
    public async Task DagPipeline_ShouldExecuteComplexDag_WithMergeAndSql()
    {
        var outputPath = GetTempPath();

        var args = new[]
        {
            "--input", "generate:10", "--alias", "b1",
            "--input", "generate:5", "--alias", "b2",
            "--from", "b1,b2", "--merge", "--alias", "merged",
            "--from", "merged", "select count(*) as c from merged", "--output", $"csv:{outputPath}"
        };

        var exitCode = await DtPipe.Program.Main(args);
        exitCode.Should().Be(0);

        var lines = await File.ReadAllLinesAsync(outputPath);
        lines.Should().HaveCount(2, "1 header + 1 count row");
        lines[0].Should().Be("c");
        lines[1].Should().Be("15");
    }
}
