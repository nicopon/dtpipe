using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;

namespace DtPipe.Tests.Shared;

/// <summary>
/// The bricks a test announces: built the way a node builds them, so a fixture's version is the one a node gives and never a literal. Linked into every test project of the distributed family.
/// </summary>
internal static class TestBricks
{
    /// <summary>The version a node gives the brick declared as <paramref name="branch"/>, YAML text (JSON included).</summary>
    public static string VersionOf(BrickKind kind, string branch) => BrickRules.VersionOf(kind, JobYaml.ParseBranch(branch));

    /// <summary>A brick announced as a node announces it: its version, and no text.</summary>
    public static BrickInfo Announced(string id, BrickKind kind, string branch, string title = "t", string description = "") =>
        new(id, kind, title, description, VersionOf(kind, branch));
}
