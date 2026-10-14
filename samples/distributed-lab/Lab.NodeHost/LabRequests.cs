using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.PipelineNode.Hosting;

namespace Lab.NodeHost;

/// <summary>
/// What the coordinator's lab mode asks of a lab node beyond its fragments, registered as a named request:
/// <c>dtpipe split</c> of a branch that reads its datasets (<c>lab.split</c>), whose sample run executes the
/// branch's transformers here. A node of the lab answers it whatever mode its fragments run in.
/// </summary>
internal sealed class LabRequests(string node)
{
    private static readonly TimeSpan SplitTimeout = TimeSpan.FromSeconds(90);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private sealed record Split(string Alias, string Job, int At);

    /// <summary>Cuts the job's one branch after stage <c>at</c>.</summary>
    public async Task<string> SplitAsync(NodeRequestContext request)
    {
        var split = JsonSerializer.Deserialize<Split>(request.Payload, Json)!;
        var dir = Path.Combine(request.DtPipe.WorkingDirectory, "splits", Guid.NewGuid().ToString("N"));
        try
        {
            if (JobYaml.Branches(JobYaml.Parse(split.Job)).ToList() is not [var (only, branch)] || only != split.Alias)
                return JsonSerializer.Serialize(new BranchSplit(null, null, $"{node} cuts one branch, sent alone."), Json);
            Directory.CreateDirectory(dir);
            var jobPath = Path.Combine(dir, "job.yaml");
            await File.WriteAllTextAsync(jobPath, BranchCut.Job(split.Alias, branch));
            var prefix = Path.Combine(dir, "cut");
            var result = await request.DtPipe.RunAsync(BranchCut.Arguments(jobPath, split.Alias, split.At, prefix), SplitTimeout);
            if (!result.Succeeded) return JsonSerializer.Serialize(new BranchSplit(null, null, result.Diagnostic), Json);
            return JsonSerializer.Serialize(new BranchSplit(
                BranchCut.Half(await File.ReadAllTextAsync(prefix + "-producer.yaml"), split.Alias),
                BranchCut.Half(await File.ReadAllTextAsync(prefix + "-consumer.yaml"), split.Alias),
                null), Json);
        }
        catch (Exception ex)
        {
            return JsonSerializer.Serialize(new BranchSplit(null, null, ex.Message), Json);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }
}
