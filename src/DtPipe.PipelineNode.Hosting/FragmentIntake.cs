using DtPipe.Coordinator.Contracts;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// What a node decides of a deployment before any <c>PipelineNode</c> registers (a registered instance is
/// admissible): the job to run, or why not. Units name bricks, and the node composes the job from its own
/// declarations; it never runs text that came from the coordinator unless its own policy or gate says so.
/// </summary>
internal static class FragmentIntake
{
    public static (string? Yaml, string? Reason) JobOf(
        FragmentDeployment deployment, FragmentPolicy policy, FragmentGate? gate, FragmentJob.BranchOfBrick brickBranch, IReadOnlyList<BrickForm> forms)
    {
        if (deployment.Units is { } units)
        {
            if (policy == FragmentPolicy.AnyFragment) return (null, "a runner has no brick to compose a job from.");
            var (composed, error) = FragmentJob.Compose(units, brickBranch);
            if (composed is null) return (null, error);
            if (FragmentJob.VersionOf(composed) != deployment.Version)
            {
                var bricks = string.Join(", ", units.Where(u => u.Brick is not null).Select(u => u.Brick).Distinct());
                return (null, $"a brick of {deployment.Fragment} ({bricks}) changed since the plan was made: distribute it again.");
            }
            return (composed, null);
        }
        if (policy == FragmentPolicy.AnyFragment) return (deployment.Yaml, null);
        var refusal = gate is null ? "this node runs the bricks its owner declared, by id, and takes no job as written." : gate(deployment, forms);
        return refusal is null ? (deployment.Yaml, null) : (null, refusal);
    }
}
