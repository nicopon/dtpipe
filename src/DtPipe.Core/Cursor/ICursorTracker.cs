namespace DtPipe.Core.Cursor;

public interface ICursorTracker
{
    CursorValue? TrackedMaxValue { get; }
}
