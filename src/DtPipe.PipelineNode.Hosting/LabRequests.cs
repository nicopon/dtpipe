using DtPipe.Coordinator.Contracts;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// What the coordinator's lab mode asks of a node beyond its fragments: a query on one of its datasets.
/// The node answers only when its own configuration turns <see cref="DtPipeNodeOptions.LabMode"/> on;
/// the coordinator's mode is the coordinator's business, never a reason for the node to read.
/// </summary>
internal sealed class LabRequests(string node, bool enabled, IReadOnlyList<NodeVariable> variables, DtPipeCommand dtpipe)
{
    public async Task<DatasetQueryResult> QueryAsync(string variable, string sql, int rows)
    {
        if (!enabled) return new DatasetQueryResult(null, $"{node} does not answer lab requests: its configuration leaves LabMode off.");
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
}
