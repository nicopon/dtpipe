namespace DtPipe.Jobs;

/// <summary>The provider-options blocks of a branch that its reader owns, and those its writer owns.</summary>
public sealed record EndpointBlocks(IReadOnlySet<string> Reader, IReadOnlySet<string> Writer);

/// <summary>
/// Which side of a branch each of its provider-options blocks belongs to, from the components behind
/// its <c>input</c> and <c>output</c>. dtpipe answers it with the reading a cut uses; a host that
/// offers no rules leaves every branch that is not a brick as written.
/// </summary>
public interface IEndpointRules
{
    EndpointBlocks Blocks(string? input, string? output, IReadOnlyList<string> blocks);
}
