using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;

namespace DtPipe.Cli.Infrastructure;

/// <summary>
/// The internal channel transports, registered apart from the reader and writer factories. Every
/// site that routes a connection string by name (<see cref="ComponentSelector"/>, then
/// <c>CanHandle</c>) iterates those collections, so a transport kept out of them is no prefix a
/// user can write: it is chosen by its capability alone, for a typed endpoint the orchestrator
/// hands to a branch.
/// </summary>
public sealed class InternalChannelTransports(
    IReadOnlyList<IStreamReaderFactory> readers,
    IReadOnlyList<IDataWriterFactory> writers)
{
    public IReadOnlyList<IStreamReaderFactory> Readers { get; } = readers;

    public IStreamReaderFactory? Reader(InternalChannelKind kind)
        => Readers.FirstOrDefault(f => f.CapabilityKind == kind);

    public IDataWriterFactory? Writer(InternalChannelKind kind)
        => writers.FirstOrDefault(f => f.CapabilityKind == kind);

    /// <summary>Whether a discovered descriptor is a transport rather than a provider.</summary>
    internal static bool Carries(Type descriptorType)
        => typeof(IInternalChannelCapable).IsAssignableFrom(descriptorType);
}
