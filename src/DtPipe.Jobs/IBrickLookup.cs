using YamlDotNet.RepresentationModel;

namespace DtPipe.Jobs;

/// <summary>
/// A brick as a card sees it. <see cref="Kind"/> is <see cref="StepKinds.Source"/> or
/// <see cref="StepKinds.Sink"/>; <see cref="Node"/>, <see cref="Version"/> and <see cref="Title"/>
/// are what the designer shows beside the card.
/// </summary>
public sealed record DesignBrick(string Key, string Kind, string? Node, string? Version, string? Title, bool Online);

/// <summary>
/// The bricks a designer may place, and how a branch is recognised as one: a brick is found by its key
/// (<c>node/id</c>), and a branch is one when it is the reference to it (<see cref="BrickRef"/>). The coordinator's
/// catalog offers its nodes' bricks; <see cref="None"/> offers nothing.
/// </summary>
public interface IBrickLookup
{
    DesignBrick? Find(string key);

    /// <summary>A copy of the branch that references the brick: <c>input: brick:&lt;key&gt;</c> for a source, <c>output: brick:&lt;key&gt;</c> for a sink.</summary>
    YamlMappingNode BranchOf(DesignBrick brick);

    /// <summary>The brick a whole branch references, or null.</summary>
    DesignBrick? Match(YamlMappingNode branch);

    static IBrickLookup None { get; } = new NoBricks();

    private sealed class NoBricks : IBrickLookup
    {
        public DesignBrick? Find(string key) => null;
        public YamlMappingNode BranchOf(DesignBrick brick) => throw new InvalidOperationException($"No brick '{brick.Key}' is on offer.");
        public DesignBrick? Match(YamlMappingNode branch) => null;
    }
}
