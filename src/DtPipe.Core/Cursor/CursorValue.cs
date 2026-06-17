namespace DtPipe.Core.Cursor;

/// <summary>Incremental cursor value, as persisted in a state file.</summary>
public record CursorValue(string Column, string Value, CursorType Type);

public enum CursorType { DateTime, Integer, String }
