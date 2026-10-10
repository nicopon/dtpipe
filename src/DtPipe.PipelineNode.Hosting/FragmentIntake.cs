using DtPipe.Coordinator.Contracts;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// What a node decides of a fragment before it spawns anything: the job to run, or why not. Units name bricks,
/// and the node composes the job from its own declarations; it never runs text that came from the coordinator
/// unless its own policy or gate says so.
/// </summary>
internal static class FragmentIntake
{
    public static (string? Yaml, string? Reason) JobOf(
        FragmentSpec spec, FragmentPolicy policy, FragmentGate? gate, FragmentJob.BranchOfBrick brickBranch, IReadOnlyList<BrickForm> forms)
    {
        if (spec.Units is not null)
            return policy == FragmentPolicy.AnyFragment ? (null, "a runner has no brick to compose a job from.") : FragmentJob.ComposeFor(spec, brickBranch);
        if (spec.Yaml is null) return (null, $"{spec.Fragment} carries neither bricks nor a job.");
        if (policy == FragmentPolicy.AnyFragment) return (spec.Yaml, null);
        var refusal = gate is null ? "this node runs the bricks its owner declared, by id, and takes no job as written." : gate(spec, forms);
        return refusal is null ? (spec.Yaml, null) : (null, refusal);
    }
}
