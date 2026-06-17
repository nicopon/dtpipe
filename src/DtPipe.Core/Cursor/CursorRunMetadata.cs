namespace DtPipe.Core.Cursor;

public record CursorRunMetadata(
    DateTime StartedAt,
    DateTime CompletedAt,
    long RowsTransferred,
    string Status);
