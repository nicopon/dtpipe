using System.Diagnostics;
using System.Text;

namespace DtPipe.Cli.Studio;

/// <summary>What a publication would refuse: <see cref="Report"/> is the runner's own words, null when it takes the job.</summary>
public sealed record PublicationVerdict(bool Ok, string? Report);

/// <summary>
/// The runner's dry run of a job, replayed on this machine: the binary the studio runs as, the command a runner runs for a
/// library save's check (<c>--dry-run 1 --edges-only</c>, each source brick bound to the rows its node sent, each sink brick
/// to a file of the run's directory), in a directory of its own. The rules are the engine's under <c>--edges-only</c>, never
/// the studio's: a refusal is what the runner's child writes on stderr, its last lines as the coordinator reports them.
/// The coordinator's own checks around that dry run (a reference no node offers, each sink brick's target) are not played here.
/// </summary>
internal static class PublicationCheck
{
    /// <summary>The bound a runner gives the same dry run.</summary>
    public static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    private const int ReportLines = 8;

    public static async Task<PublicationVerdict> RunAsync(
        string directory, string yaml, IReadOnlyList<(string Alias, byte[] Arrow)> sources, IReadOnlyList<string> sinks, CancellationToken ct)
    {
        await File.WriteAllTextAsync(Path.Combine(directory, "job.yaml"), yaml, new UTF8Encoding(false), ct);
        // A bound location under --edges-only is a bare file of the working directory, and one ending in .arrow is a file to the
        // arrow: reader, not a stream.
        foreach (var (alias, arrow) in sources) await File.WriteAllBytesAsync(Path.Combine(directory, alias + ".arrows"), arrow, ct);
        var (program, prefix) = Self();
        var start = new ProcessStartInfo(program)
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = directory,
        };
        foreach (var arg in prefix) start.ArgumentList.Add(arg);
        string[] args =
        [
            "--job", "job.yaml", "--dry-run", "1", "--no-stats", "--edges-only",
            .. sources.Count > 0 ? ["--bind-input", string.Join(',', sources.Select(s => $"{s.Alias}={s.Alias}.arrows"))] : Array.Empty<string>(),
            .. sinks.Count > 0 ? ["--bind-output", string.Join(',', sinks.Select(a => $"{a}={a}.sink"))] : Array.Empty<string>(),
        ];
        foreach (var arg in args) start.ArgumentList.Add(arg);
        start.Environment["NO_COLOR"] = "1";
        start.Environment["DTPIPE_NO_TUI"] = "1";

        using var process = Process.Start(start) ?? throw new StudioRefusal(502, "dtpipe did not start for the publication's check.");
        // Nobody answers the child: a prompt it would ask fails instead of waiting on the studio's terminal.
        process.StandardInput.Close();
        var stdout = process.StandardOutput.ReadToEndAsync(CancellationToken.None);
        var stderr = process.StandardError.ReadToEndAsync(CancellationToken.None);
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bounded.CancelAfter(Bound);
        try
        {
            await process.WaitForExitAsync(bounded.Token);
        }
        catch (OperationCanceledException)
        {
            try { process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
            ct.ThrowIfCancellationRequested();
            throw new StudioRefusal(504, $"The runner's dry run did not end within {Bound.TotalSeconds:F0}s.");
        }
        await stdout;
        if (process.ExitCode == 0) return new PublicationVerdict(true, null);
        var lines = (await stderr).Split('\n').Select(l => l.TrimEnd()).Where(l => l.Length > 0).TakeLast(ReportLines);
        return new PublicationVerdict(false, string.Join('\n', lines));
    }

    /// <summary>
    /// The program that runs this dtpipe: the single-file binary or apphost the process is, else the dotnet host with this
    /// assembly (<c>dotnet DtPipe.dll</c>, a tool package, a test host).
    /// </summary>
    internal static (string Program, string[] Prefix) Self()
    {
        var self = Environment.ProcessPath;
        var dll = typeof(PublicationCheck).Assembly.Location;
        if (string.IsNullOrEmpty(dll)) return (self ?? "dtpipe", []);
        var name = self is null ? "" : Path.GetFileName(self);
        if (name.Equals("dtpipe", StringComparison.OrdinalIgnoreCase) || name.Equals("dtpipe.exe", StringComparison.OrdinalIgnoreCase))
            return (self!, []);
        var host = name.Equals("dotnet", StringComparison.OrdinalIgnoreCase) || name.Equals("dotnet.exe", StringComparison.OrdinalIgnoreCase)
            ? self!
            : "dotnet";
        return (host, [dll]);
    }
}
