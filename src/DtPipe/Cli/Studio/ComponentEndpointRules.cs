using DtPipe.Cli.Split;
using DtPipe.Core.Abstractions;
using DtPipe.Jobs;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The designer's reading of a branch's endpoints: dtpipe's own readers and writers, through the
/// reading a cut uses (<see cref="JobCutter.OwnersOf"/>), so a card never places a block on a side
/// <c>dtpipe split</c> would not.
/// </summary>
public sealed class ComponentEndpointRules(IEnumerable<IStreamReaderFactory> readers, IEnumerable<IDataWriterFactory> writers) : IEndpointRules
{
    public EndpointBlocks Blocks(string? input, string? output, IReadOnlyList<string> blocks)
    {
        var owners = JobCutter.OwnersOf(input, output, blocks, readers, writers);
        return new EndpointBlocks(owners.Reader, owners.Writer);
    }
}
