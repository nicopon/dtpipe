using DtPipe.Coordinator.Contracts;

namespace DtPipe.Tests.Shared;

/// <summary>
/// The bricks a test announces: built the way a node builds them, so a fixture's version is the keyed one a node of
/// <see cref="Key"/> gives and never a literal. Linked into every test project of the distributed family.
/// </summary>
internal static class TestBricks
{
    /// <summary>The brick key the test nodes share.</summary>
    public const string Key = "test-brick-key-0123456789";

    /// <summary>The version a node keyed with <see cref="Key"/> gives the brick declared as <paramref name="branchJson"/>.</summary>
    public static string VersionOf(BrickKind kind, string branchJson) => BrickRules.VersionOf(kind, branchJson, Key);

    /// <summary>A brick announced as a node announces it: its version, and no text.</summary>
    public static BrickInfo Announced(string id, BrickKind kind, string branchJson, string title = "t", string description = "") =>
        new(id, kind, title, description, VersionOf(kind, branchJson));
}
