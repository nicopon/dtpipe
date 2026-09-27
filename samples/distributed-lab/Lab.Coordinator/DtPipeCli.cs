using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace DtPipe.Lab.Coordinator;

public sealed record CliResult(int ExitCode, string Stdout, string Stderr)
{
    public bool Succeeded => ExitCode == 0;

    /// <summary>The last non-empty stderr lines: where dtpipe states why it refused.</summary>
    public string Diagnostic(int lines = 6) => string.Join('\n',
        Stderr.Split('\n').Select(l => l.TrimEnd()).Where(l => l.Length > 0).TakeLast(lines));
}

/// <summary>
/// Runs the dtpipe binary the way a user would. The lab never links dtpipe's engine: splitting and
/// querying go through the published command line.
/// </summary>
public sealed partial class DtPipeCli(LabOptions options, NodeInventory inventory)
{
    public async Task<CliResult> RunAsync(IEnumerable<string> args, string workingDirectory, TimeSpan timeout, CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = options.DtPipeExecutable,
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var arg in args) psi.ArgumentList.Add(arg);

        // A fragment's job names its databases through ${{VAR}}; on this single machine the
        // coordinator can resolve every node's variable, which is what lets `split` sample a source.
        foreach (var (name, path) in inventory.AllDatasetVariables())
            psi.Environment[name] = path;
        psi.Environment["NO_COLOR"] = "1";

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("dtpipe did not start.");
        var stdout = process.StandardOutput.ReadToEndAsync(ct);
        var stderr = process.StandardError.ReadToEndAsync(ct);

        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(timeout);
        try
        {
            await process.WaitForExitAsync(bounded.Token);
        }
        catch (OperationCanceledException)
        {
            try { process.Kill(entireProcessTree: true); } catch { }
            throw new TimeoutException($"dtpipe {string.Join(' ', psi.ArgumentList)} did not finish within {timeout.TotalSeconds:F0}s.");
        }

        return new CliResult(process.ExitCode, StripAnsi(await stdout), StripAnsi(await stderr));
    }

    private static string StripAnsi(string text) => Ansi().Replace(text, "");

    [GeneratedRegex(@"\x1B\[[0-9;?]*[ -/]*[@-~]")]
    private static partial Regex Ansi();
}
