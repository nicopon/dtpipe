using YamlDotNet.Core;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Contracts;

/// <summary>
/// How one branch is handed to <c>dtpipe split</c> on its own, by the node that hosts what it reads or
/// by the coordinator: the rest of the job reads other nodes' data, which only those nodes can open.
/// dtpipe refuses a branch that nothing reads and that writes nothing, so a reader to <c>null:</c>
/// stands in for the rest of the job. Only the cut branch's two halves come back; the coordinator
/// puts the consumer half where the branch was and leaves every other branch as written.
/// </summary>
public static class BranchCut
{
    /// <summary>The job <c>dtpipe split --branch <paramref name="alias"/></c> runs on.</summary>
    public static string Job(string alias, YamlMappingNode branch)
    {
        var job = new YamlMappingNode { { alias, JobYaml.CloneBranch(branch) } };
        if (!JobYaml.Has(branch, "output"))
        {
            var reader = alias + "-split-reader";
            job.Add(reader, new YamlMappingNode
            {
                { "from", alias },
                { "output", new YamlScalarNode("null:") { Style = ScalarStyle.DoubleQuoted } },
            });
        }
        return JobYaml.Serialize(job);
    }

    /// <summary>The cut branch's half in a job file <c>dtpipe split</c> wrote, as a one-branch job.</summary>
    public static string Half(string writtenJob, string alias)
    {
        var branch = JobYaml.Branches(JobYaml.Parse(writtenJob)).Single(b => b.Alias == alias).Branch;
        return JobYaml.Serialize(new YamlMappingNode { { alias, branch } });
    }

    /// <summary>The arguments of the cut: <c>&lt;prefix&gt;-producer.yaml</c> and <c>&lt;prefix&gt;-consumer.yaml</c> are its output.</summary>
    public static string[] Arguments(string jobPath, string alias, int at, string prefix) =>
        ["split", jobPath, "--branch", alias, "--at", at.ToString(), "--out", prefix, "--acknowledge"];
}
