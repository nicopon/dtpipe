using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// A factory for creating XStreamers. An XStreamer is a component capable
/// of mixing multiple input data streams into a single output stream.
/// </summary>
public interface IXStreamerFactory : IProviderDescriptor<IStreamReader>
{
    /// <summary>
    /// Declares the channel protocol this XStreamer requires for its upstream input branches.
    /// The orchestrator uses this to register the correct type of memory channel (native object[]
    /// or Arrow RecordBatch) without any CLI flag inspection.
    /// </summary>
    XStreamerChannelMode ChannelMode { get; }
}
