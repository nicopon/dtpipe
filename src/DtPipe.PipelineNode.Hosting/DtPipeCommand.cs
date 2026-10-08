using System.Diagnostics;
using System.Text;

namespace DtPipe.PipelineNode.Hosting;

internal sealed record DtPipeCommandResult(int ExitCode, byte[] Output, string Stderr)
{
    public bool Succeeded => ExitCode == 0;

    public string Stdout => Encoding.UTF8.GetString(Output);

    /// <summary>The last non-empty stderr lines: where dtpipe states why it refused.</summary>
    public string Diagnostic => Tail(4);

    public string Tail(int lines) => string.Join('\n',
        Stderr.Split('\n').Select(l => l.TrimEnd()).Where(l => l.Length > 0).TakeLast(lines));
}

/// <summary>
/// Runs the dtpipe binary for what the node reads itself rather than as a fragment (a brick's schema
/// and preview, the coordinator's lab requests), with the environment its fragments inherit, so a
/// command names a database through <c>${{VARIABLE}}</c> and its value never enters an argument.
/// </summary>
internal sealed class DtPipeCommand(string dtpipe, string workingDirectory, IReadOnlyDictionary<string, string> environment)
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    public string WorkingDirectory => workingDirectory;

    /// <summary>
    /// What the binary says of itself (<c>dtpipe --version</c>, its name left out), or null when it does
    /// not start, fails or answers anything else: a version is information, never a reason to stop.
    /// </summary>
    public async Task<string?> VersionAsync(CancellationToken ct)
    {
        try
        {
            var result = await RunAsync(["--version"], ct, TimeSpan.FromSeconds(15));
            var line = result.Stdout.Split('\n', 2)[0].Trim();
            return result.Succeeded && line.StartsWith("dtpipe ", StringComparison.Ordinal) && line.Length > "dtpipe ".Length
                ? line["dtpipe ".Length..].Trim()
                : null;
        }
        catch (Exception) when (!ct.IsCancellationRequested)
        {
            return null;
        }
    }

    public async Task<DtPipeCommandResult> RunAsync(IEnumerable<string> args, CancellationToken ct, TimeSpan? timeout = null, string? inDirectory = null)
    {
        var bound = timeout ?? DefaultTimeout;
        var psi = new ProcessStartInfo
        {
            FileName = dtpipe,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = inDirectory ?? workingDirectory,
        };
        foreach (var (name, value) in environment) psi.Environment[name] = value;
        foreach (var arg in args) psi.ArgumentList.Add(arg);
        psi.Environment["NO_COLOR"] = "1";
        psi.Environment["DTPIPE_NO_TUI"] = "1";

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("dtpipe did not start.");
        // Bytes, not text: a command may write an Arrow IPC stream on stdout.
        var output = new MemoryStream();
        var stdout = process.StandardOutput.BaseStream.CopyToAsync(output, ct);
        var stderr = process.StandardError.ReadToEndAsync(ct);
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(bound);
        try
        {
            await process.WaitForExitAsync(bounded.Token);
        }
        catch (OperationCanceledException)
        {
            try { process.Kill(entireProcessTree: true); } catch { }
            throw new TimeoutException($"dtpipe did not answer within {bound.TotalSeconds:F0}s.");
        }
        await stdout;
        return new DtPipeCommandResult(process.ExitCode, output.ToArray(), await stderr);
    }
}
