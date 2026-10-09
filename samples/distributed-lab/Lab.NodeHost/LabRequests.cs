using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.PipelineNode.Hosting;

namespace Lab.NodeHost;

/// <summary>
/// What the coordinator's lab mode asks of a lab node beyond its fragments, registered as named requests: a
/// query on one of its datasets (<c>lab.query</c>), and <c>dtpipe split</c> of a branch that reads them
/// (<c>lab.split</c>), whose sample run executes the branch's transformers here. Only a variable given an
/// engine is queried, and a node of the lab answers both whatever mode its fragments run in.
/// </summary>
internal sealed class LabRequests(string node, IReadOnlyList<(string Variable, string Engine)> datasets)
{
    private static readonly TimeSpan SplitTimeout = TimeSpan.FromSeconds(90);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private sealed record Query(string Variable, string Sql, int Rows);

    private sealed record Split(string Alias, string Job, int At);

    public async Task<string> QueryAsync(NodeRequestContext request)
    {
        var query = JsonSerializer.Deserialize<Query>(request.Payload, Json)!;
        return JsonSerializer.Serialize(await RunQueryAsync(request.DtPipe, query), Json);
    }

    private async Task<DatasetQueryResult> RunQueryAsync(NodeDtPipe dtpipe, Query query)
    {
        var dataset = datasets.FirstOrDefault(d => d.Variable == query.Variable);
        if (dataset.Engine is null) return new DatasetQueryResult(null, $"{node} hosts no dataset at {query.Variable}.");
        try
        {
            var result = await dtpipe.RunAsync(
                ["--input", dataset.Engine + ":${{" + query.Variable + "}}", "--query", query.Sql, "--limit", query.Rows.ToString(), "--output", "csv:-", "--no-stats"]);
            return result.Succeeded ? new DatasetQueryResult(result.Stdout, null) : new DatasetQueryResult(null, result.Diagnostic);
        }
        catch (Exception ex)
        {
            return new DatasetQueryResult(null, ex.Message);
        }
    }

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
