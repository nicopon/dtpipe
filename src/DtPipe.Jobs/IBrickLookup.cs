using YamlDotNet.RepresentationModel;

namespace DtPipe.Jobs;

/// <summary>
/// A brick as a card sees it. <see cref="Kind"/> is <see cref="StepKinds.Source"/> or
/// <see cref="StepKinds.Sink"/>; <see cref="Node"/>, <see cref="Version"/> and <see cref="Title"/>
/// are what the designer shows beside the card.
/// </summary>
public sealed record DesignBrick(string Key, string Kind, string? Node, string? Version, string? Title, bool Online);

/// <summary>
/// The bricks a designer may place, and how a branch is recognised as one: by its content, never by
/// a name. The coordinator's catalog offers its nodes' bricks; <see cref="None"/> offers nothing.
/// </summary>
public interface IBrickLookup
{
    DesignBrick? Find(string key);

    /// <summary>A copy of the branch the brick stands for, as its owner declared it.</summary>
    YamlMappingNode BranchOf(DesignBrick brick);

    /// <summary>The brick a whole branch is, or null.</summary>
    DesignBrick? Match(YamlMappingNode branch);

    /// <summary>
    /// The brick a branch starts with (<paramref name="kind"/> is <see cref="StepKinds.Source"/>) or
    /// ends with (<see cref="StepKinds.Sink"/>), around work of its own, or null.
    /// </summary>
    DesignBrick? Contained(YamlMappingNode branch, string kind);

    static IBrickLookup None { get; } = new NoBricks();

    private sealed class NoBricks : IBrickLookup
    {
        public DesignBrick? Find(string key) => null;
        public YamlMappingNode BranchOf(DesignBrick brick) => throw new InvalidOperationException($"No brick '{brick.Key}' is on offer.");
        public DesignBrick? Match(YamlMappingNode branch) => null;
        public DesignBrick? Contained(YamlMappingNode branch, string kind) => null;
    }
}
