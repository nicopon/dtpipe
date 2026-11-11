using DtPipe.Jobs;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Contracts;

/// <summary>
/// What a runner's fragment may read and write: its <c>arrow:</c> edges and nothing else. A runner hosts no data, and the
/// fragments of every run share its host, so a file or a database it reaches would carry rows between two flows the
/// coordinator never judged together. The coordinator refuses such a fragment when it plans and the runner again before it
/// launches one; the runner's child runs under <c>--edges-only</c>, which the engine enforces itself. Processors and
/// transformers stay: they read the branches the fragment declares.
/// </summary>
public static class RunnerEndpoints
{
    private const string Why = "a runner's fragment reads and writes its arrow: edges and nothing else";

    // What a branch keeps on the host's disk beside its reader and writer, as the job file names it.
    private static readonly string[] Places = ["checkpoint", "from-checkpoint", "contract-save", "from-contract", "metrics-path", "cursor", "state"];

    /// <summary>
    /// The first endpoint of the fragment <paramref name="yaml"/> that is no edge of <paramref name="edges"/>, naming its
    /// branch and the endpoint, or <c>null</c> when every input and output is <c>arrow:-</c> on an edge of its alias and
    /// direction. A job that cannot be read is refused too.
    /// </summary>
    public static string? FragmentRefusal(string yaml, IReadOnlyList<FragmentEdge> edges) =>
        Refusals(yaml, edges).FirstOrDefault();

    /// <summary>Every endpoint of the fragment that is no edge, each naming its branch and the endpoint.</summary>
    public static IReadOnlyList<string> Refusals(string yaml, IReadOnlyList<FragmentEdge> edges)
    {
        YamlMappingNode job;
        try { job = JobYaml.Parse(yaml); }
        catch (Exception ex) { return [$"the fragment is not readable: {ex.Message}"]; }

        bool IsEdge(string alias, FragmentEdgeDirection direction) =>
            edges.Any(e => e.Alias == alias && e.Direction == direction);

        var refusals = new List<string>();
        foreach (var (alias, branch) in JobYaml.Branches(job))
        {
            if (JobYaml.Scalar(branch, JobKeys.Input) is { } input
                && !(input == BrickRules.ArrowEndpoint && IsEdge(alias, FragmentEdgeDirection.Inbound)))
                refusals.Add($"'{alias}' reads '{input}': {Why}.");
            else if (JobYaml.Has(branch, JobKeys.Input) && JobYaml.Scalar(branch, JobKeys.Input) is null)
                refusals.Add($"'{alias}' reads something other than an edge: {Why}.");

            if (JobYaml.Scalar(branch, JobKeys.Output) is { } output
                && !(output == BrickRules.ArrowEndpoint && IsEdge(alias, FragmentEdgeDirection.Outbound)))
                refusals.Add($"'{alias}' writes '{output}': {Why}.");
            else if (JobYaml.Has(branch, JobKeys.Output) && JobYaml.Scalar(branch, JobKeys.Output) is null)
                refusals.Add($"'{alias}' writes something other than an edge: {Why}.");

            foreach (var key in Places.Where(k => JobYaml.Has(branch, k)))
                refusals.Add($"'{alias}' sets {key}: {Why}.");

            if (JobYaml.Mapping(branch, JobKeys.ProviderOptions) is { } options
                && JobYaml.Mapping(options, "sql") is { } sql && JobYaml.Has(sql, "duck-init"))
                refusals.Add($"'{alias}' runs a duck-init script, which reaches what its edges do not: {Why}.");
        }
        return refusals;
    }
}
