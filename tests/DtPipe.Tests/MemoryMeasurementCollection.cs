using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// Tests that read <c>GC.GetTotalMemory</c> across a workload must not share the process with
/// other tests: their allocations land in the measured delta. A collection that disables
/// parallelisation runs alone.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class MemoryMeasurementCollection
{
	public const string Name = "memory-measurement";
}
