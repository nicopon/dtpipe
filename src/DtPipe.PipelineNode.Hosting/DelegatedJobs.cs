using System.Text;
using System.Text.RegularExpressions;
using DtPipe.Coordinator.Contracts;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// The dtpipe run the coordinator asks of a runner because it runs none itself: a library save's job
/// dry-run over one-row samples. Only a host that runs any fragment answers; any other says so. Each
/// request works in a directory of its own, removed after.
/// </summary>
internal sealed partial class DelegatedJobs(string node, bool enabled, DtPipeCommand dtpipe)
{
    private static readonly TimeSpan DryRunTimeout = TimeSpan.FromSeconds(60);

    private string Refused => $"{node} runs no job for the coordinator: only a runner does.";

    [GeneratedRegex("^[A-Za-z][A-Za-z0-9_-]*$")]
    private static partial Regex Alias();

    public async Task<JobCheckResult> ValidateAsync(JobCheck check)
    {
        if (!enabled) return new JobCheckResult(false, Refused, new Dictionary<string, string>());
        // An alias names a file in the request's directory, so it is a plain word.
        var odd = check.Samples.Keys.Concat(check.Sinks).FirstOrDefault(a => !Alias().IsMatch(a));
        if (odd is not null) return new JobCheckResult(false, $"'{odd}' is not an alias: a letter first, then letters, digits, '_' or '-'.", new Dictionary<string, string>());

        var dir = Path.Combine(dtpipe.WorkingDirectory, "validate", Guid.NewGuid().ToString("N"));
        try
        {
            Directory.CreateDirectory(dir);
            await File.WriteAllTextAsync(Path.Combine(dir, "job.yaml"), check.Job, new UTF8Encoding(false));
            foreach (var (alias, bytes) in check.Samples) await File.WriteAllBytesAsync(Path.Combine(dir, JobCheck.SampleFile(alias)), bytes);
            // Every sink is bound to a file of its own: the writer is neutralised, so none is written.
            string[] args =
            [
                "--job", "job.yaml", "--dry-run", "1", "--no-stats",
                .. check.Samples.Count > 0 ? ["--bind-input", string.Join(',', check.Samples.Keys.Select(a => $"{a}={JobCheck.SampleFile(a)}"))] : Array.Empty<string>(),
                .. check.Sinks.Count > 0 ? ["--bind-output", string.Join(',', check.Sinks.Select(a => $"{a}={a}.sink"))] : Array.Empty<string>(),
            ];
            var result = await dtpipe.RunAsync(args, CancellationToken.None, DryRunTimeout, dir);
            if (!result.Succeeded) return new JobCheckResult(false, result.Tail(8), new Dictionary<string, string>());
            var contracts = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var alias in check.Sinks)
            {
                var path = Path.Combine(dir, JobCheck.ContractFile(alias));
                if (File.Exists(path)) contracts[alias] = await File.ReadAllTextAsync(path);
            }
            return new JobCheckResult(true, "", contracts);
        }
        catch (Exception ex)
        {
            return new JobCheckResult(false, ex.Message, new Dictionary<string, string>());
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }
}
