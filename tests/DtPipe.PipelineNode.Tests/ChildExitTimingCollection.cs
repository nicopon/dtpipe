using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// Tests whose verdict depends on how soon a child starts or how soon its end is seen run one class at a time, after the
/// parallel ones. While a child is being started, the process observes no other child's end until that start returns, so
/// such a test beside another class's launch can fail on that launch's speed. While a class of this collection runs, no other
/// class of the project starts a child; the children of one test still start together. A class with an elapsed-time bound
/// on a child, or a short run timeout, joins it.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ChildExitTimingCollection
{
    public const string Name = "child-exit-timing";
}
