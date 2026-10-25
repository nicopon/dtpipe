namespace DtPipe.Core.Abstractions;

/// <summary>
/// Marker for stream-processor factories (the --sql / --merge family).
/// Lets the host classify processors without comparing ComponentName strings.
/// </summary>
public interface IStreamProcessorMarker
{
}
