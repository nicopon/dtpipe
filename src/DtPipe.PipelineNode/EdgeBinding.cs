namespace DtPipe.PipelineNode;

public enum EdgeDirection
{
    /// <summary>Reads the local child's output and relays it out through TransportR.</summary>
    Outbound,

    /// <summary>Relays bytes received through TransportR into the local child's input.</summary>
    Inbound,
}

/// <summary>
/// One frontier of the hosted fragment. A node relays each edge through the child's own stdin
/// (inbound) or stdout (outbound), so at most one edge per <see cref="EdgeDirection"/> is
/// supported — a fragment with several inbound or several outbound edges needs a FIFO per extra
/// edge, which <see cref="PipelineNode"/> does not yet implement.
/// </summary>
public sealed record EdgeBinding(string Alias, EdgeDirection Direction);
