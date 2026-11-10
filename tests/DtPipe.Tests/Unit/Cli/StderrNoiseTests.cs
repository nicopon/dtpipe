using System;
using System.IO;
using System.Threading.Tasks;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// F17 noise gate — representative pipelines must run with a SILENT stderr.
/// Any "[dtpipe] Warning" here means a silent-degradation path leaked back in
/// (unbound options, unrecognized keys, missing registrations…). These runs
/// exercise the paths that historically warned: linear, multi-instance
/// transformers, YAML round-trip and the stream-processor branch whose
/// factory aliases its OptionsType to PipelineOptions.
/// </summary>
[Collection("console-serial")]
public class StderrNoiseTests : IAsyncLifetime
{
    private readonly List<string> _cleanupPaths = new();
    private string _workDir = "";

    public ValueTask InitializeAsync()
    {
        _workDir = Path.Combine(Path.GetTempPath(), "dtpipe-stderr-noise-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_workDir);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        try { Directory.Delete(_workDir, recursive: true); } catch { /* ignore */ }
        foreach (var path in _cleanupPaths)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { /* ignore */ }
        }
        return ValueTask.CompletedTask;
    }

    private static async Task<(int ExitCode, string Stderr)> RunCapturingStderr(params string[] args)
    {
        var originalError = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        try
        {
            var exitCode = await Program.Main(args);
            return (exitCode, captured.ToString());
        }
        finally
        {
            Console.SetError(originalError);
        }
    }

    private string TempFile(string name)
    {
        var path = Path.Combine(_workDir, name);
        _cleanupPaths.Add(path);
        return path;
    }

    [Fact]
    public async Task Linear_Pipeline_Is_Silent_On_Stderr()
    {
        var output = TempFile("linear.csv");

        var (exitCode, stderr) = await RunCapturingStderr(
            "-i", "generate:20", "-o", $"csv:{output}", "--no-stats");

        exitCode.Should().Be(0);
        stderr.Should().NotContain("[dtpipe] Warning", "a clean linear run must not warn");
    }

    [Fact]
    public async Task Multi_Instance_Transformers_Are_Silent_On_Stderr()
    {
        var output = TempFile("transformers.csv");

        // Two fake instances + a filter — the documented multi-instance idiom.
        var (exitCode, stderr) = await RunCapturingStderr(
            "-i", "generate:20",
            "--fake", "A:name.firstName", "--fake-seed-row",
            "--fake", "B:name.lastName", "--fake-seed-row",
            "--filter", "row.GenerateIndex != null",
            "--drop", "GenerateIndex",
            "-o", $"csv:{output}", "--no-stats");

        exitCode.Should().Be(0);
        stderr.Should().NotContain("[dtpipe] Warning");
    }

    [Fact]
    public async Task Stream_Processor_Branch_Is_Silent_On_Stderr()
    {
        // Regression guard: the stream-processor reader adapter aliases its factory
        // OptionsType to PipelineOptions — the pre-flight probes used to miss and warn.
        var output = TempFile("sql_branch.csv");

        var (exitCode, stderr) = await RunCapturingStderr(
            "-i", "generate:50", "--alias", "t",
            "--from", "t", "--sql", "SELECT COUNT(*) AS cnt FROM t",
            "-o", $"csv:{output}", "--no-stats");

        exitCode.Should().Be(0);
        stderr.Should().NotContain("[dtpipe] Warning");
        stderr.Should().NotContain("no options of type", "PipelineOptions must be registered before any probe");
    }

    [Fact]
    public async Task ExportJob_RoundTrip_Is_Silent_On_Stderr()
    {
        var output = TempFile("roundtrip.csv");
        var jobFile = TempFile("job.yaml");

        var (_, exportStderr) = await RunCapturingStderr(
            "-i", "generate:10",
            "--fake", "Name:name.firstName",
            "-o", $"csv:{output}", "--export-job", jobFile);

        exportStderr.Should().NotContain("[dtpipe] Warning");

        File.Exists(jobFile).Should().BeTrue();

        var (replayExit, replayStderr) = await RunCapturingStderr("--job", jobFile);
        replayExit.Should().Be(0);
        replayStderr.Should().NotContain("[dtpipe] Warning");
    }

    [Fact]
    public async Task ExportJob_Without_Filename_Exits_NonZero_And_Creates_No_True_File()
    {
        var trueFile = Path.Combine(Directory.GetCurrentDirectory(), "true");
        _cleanupPaths.Add(trueFile);
        if (File.Exists(trueFile)) File.Delete(trueFile);

        var (exitCode, stderr) = await RunCapturingStderr(
            "-i", "generate:10",
            "-o", "null:",
            "--export-job");

        exitCode.Should().Be(1);
        stderr.Should().Contain("[ERROR]");
        stderr.Should().Contain("--export-job");
        stderr.Should().Contain("mandatory");
        File.Exists(trueFile).Should().BeFalse();
    }
}
