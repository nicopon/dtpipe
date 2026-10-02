namespace DtPipe.PipelineNode;

public enum EdgeDirection
{
    /// <summary>Reads the local child's output and relays it out through TransportR.</summary>
    Outbound,

    /// <summary>Relays bytes received through TransportR into the local child's input.</summary>
    Inbound,
}

/// <summary>
/// One frontier of the hosted fragment. The first edge of each <see cref="EdgeDirection"/> rides
/// the child's own stdin (inbound) or stdout (outbound); every further edge in that direction gets
/// its own named pipe (<see cref="System.IO.Pipes.NamedPipeServerStream"/>, never a POSIX FIFO —
/// the mechanism has to reach Windows too), created by <see cref="PipelineNode"/> before the child
/// that connects to it as client ever starts. <see cref="Alias"/> is also the alias the hosted
/// job's own branch uses (<c>--bind-input</c>/<c>--bind-output</c> substitutes by that same key).
/// </summary>
public sealed record EdgeBinding(string Alias, EdgeDirection Direction);
