using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// Tests that start a real Terminal.Gui application must not run in parallel. An application owns
/// the terminal and the toolkit keeps per-process state behind it; two overlapping instances would
/// fail for reasons that have nothing to do with the code under test.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TerminalGuiCollection
{
    public const string Name = "terminal-gui";
}
