using DtPipe.Coordinator.Contracts;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// What the coordinator's lab mode asks of a node beyond its fragments: a query on one of its
/// datasets, and <c>dtpipe split</c> of a branch that reads them, whose sample run executes the
/// branch's transformers here. The node answers only when its own configuration turns
/// <see cref="DtPipeNodeOptions.LabMode"/> on; the coordinator's mode is the coordinator's business,
/// never a reason for the node to read.
/// </summary>
internal sealed class LabRequests(string node, bool enabled, IReadOnlyList<NodeVariable> variables, DtPipeCommand dtpipe)
{
    private static readonly TimeSpan SplitTimeout = TimeSpan.FromSeconds(90);

    private string Refused => $"{node} does not answer lab requests: its configuration leaves LabMode off.";

    public async Task<DatasetQueryResult> QueryAsync(string variable, string sql, int rows)
    {
        if (!enabled) return new DatasetQueryResult(null, Refused);
        var dataset = variables.FirstOrDefault(v => v.Name == variable && v.Engine is not null);
        if (dataset is null) return new DatasetQueryResult(null, $"{node} hosts no dataset at {variable}.");
        try
        {
            var result = await dtpipe.RunAsync(
                ["--input", dataset.Engine + ":${{" + variable + "}}", "--query", sql, "--limit", rows.ToString(), "--output", "csv:-", "--no-stats"],
                CancellationToken.None);
            return result.Succeeded ? new DatasetQueryResult(result.Stdout, null) : new DatasetQueryResult(null, result.Diagnostic);
        }
        catch (Exception ex)
        {
            return new DatasetQueryResult(null, ex.Message);
        }
    }

    /// <summary>Cuts <paramref name="job"/>'s one branch, <paramref name="alias"/>, after stage <paramref name="at"/>.</summary>
    public async Task<BranchSplit> SplitAsync(string alias, string job, int at)
    {
        if (!enabled) return new BranchSplit(null, null, Refused);
        var dir = Path.Combine(dtpipe.WorkingDirectory, "splits", Guid.NewGuid().ToString("N"));
        try
        {
            if (JobYaml.Branches(JobYaml.Parse(job)).ToList() is not [var (only, branch)] || only != alias)
                return new BranchSplit(null, null, $"{node} cuts one branch, sent alone.");
            Directory.CreateDirectory(dir);
            var jobPath = Path.Combine(dir, "job.yaml");
            await File.WriteAllTextAsync(jobPath, BranchCut.Job(alias, branch));
            var prefix = Path.Combine(dir, "cut");
            var result = await dtpipe.RunAsync(BranchCut.Arguments(jobPath, alias, at, prefix), CancellationToken.None, SplitTimeout);
            if (!result.Succeeded) return new BranchSplit(null, null, result.Diagnostic);
            return new BranchSplit(
                BranchCut.Half(await File.ReadAllTextAsync(prefix + "-producer.yaml"), alias),
                BranchCut.Half(await File.ReadAllTextAsync(prefix + "-consumer.yaml"), alias),
                null);
        }
        catch (Exception ex)
        {
            return new BranchSplit(null, null, ex.Message);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }
}
