using System.Diagnostics;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// <see cref="BrickRules.Processors"/> lists the <c>provider-options</c> keys that name a processor, so that a data node
/// can refuse a branch that runs one. dtpipe's own listing is the live source: a processor dtpipe adds or renames fails
/// this test until the list follows.
/// </summary>
public sealed class BrickRulesProcessorsTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task The_Processors_A_Data_Node_Refuses_Are_The_Stream_Processors_Dtpipe_Lists()
    {
        var listed = await StreamProcessorsAsync();

        Assert.NotEmpty(listed);
        Assert.Equal(listed.Order(StringComparer.Ordinal), BrickRules.Processors.Order(StringComparer.Ordinal));
    }

    /// <summary>Runs the real binary, reads its whole output, and fails by name when it does not answer in time or exits non-zero.</summary>
    private static async Task<IReadOnlyList<string>> StreamProcessorsAsync()
    {
        var start = new ProcessStartInfo(DtPipeExecutableLocator.Path, ["providers", "--format", "json"])
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(start) ?? throw new InvalidOperationException("dtpipe providers did not start.");
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        bound.CancelAfter(Bound);

        var output = process.StandardOutput.ReadToEndAsync(bound.Token);
        var errors = process.StandardError.ReadToEndAsync(bound.Token);
        try
        {
            await process.WaitForExitAsync(bound.Token);
            var json = await output;
            var stderr = await errors;
            Assert.True(process.ExitCode == 0, $"dtpipe providers --format json exited {process.ExitCode}: {stderr}");
            return JsonDocument.Parse(json).RootElement.EnumerateArray()
                .Where(p => p.GetProperty("kind").GetString() == "stream-processor")
                .Select(p => p.GetProperty("name").GetString()!)
                .ToList();
        }
        catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"dtpipe providers --format json did not answer within {Bound}.");
        }
        finally
        {
            if (!process.HasExited) process.Kill(entireProcessTree: true);
        }
    }
}
