using System;
using System.IO;
using System.Threading.Tasks;
using DtPipe.Core.Cursor;
using Xunit;

namespace DtPipe.Tests.Unit.Cursor;

public class CursorStateStoreTests : IAsyncLifetime
{
    private string? _tempDir;

    public ValueTask InitializeAsync()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DtPipe_CursorStateStoreTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (_tempDir != null && Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, true);
        }
        return ValueTask.CompletedTask;
    }

    private string GetTempPath(string relativePath)
    {
        return Path.Combine(_tempDir!, relativePath);
    }

    [Fact]
    public void Read_FileDoesNotExist_ReturnsNull()
    {
        var path = GetTempPath("nonexistent.sync");
        var result = CursorStateStore.Read(path);
        Assert.Null(result);
    }

    [Fact]
    public void Save_ThenRead_RoundTrips()
    {
        var path = GetTempPath("test.sync");
        var cursor = new CursorValue("updated_at", "2026-06-16T12:00:00Z", CursorType.DateTime);
        var meta = new CursorRunMetadata(DateTime.UtcNow.AddMinutes(-5), DateTime.UtcNow, 42, "success");

        CursorStateStore.Save(path, cursor, meta);
        var readCursor = CursorStateStore.Read(path);

        Assert.NotNull(readCursor);
        Assert.Equal("updated_at", readCursor.Column);
        Assert.Equal("2026-06-16T12:00:00Z", readCursor.Value);
        Assert.Equal(CursorType.DateTime, readCursor.Type);
    }

    [Fact]
    public void Save_CreatesDirectoryIfNeeded()
    {
        var path = GetTempPath("subdir/test.sync");
        var cursor = new CursorValue("id", "12345", CursorType.Integer);
        var meta = new CursorRunMetadata(DateTime.UtcNow, DateTime.UtcNow, 0, "success");

        CursorStateStore.Save(path, cursor, meta);
        Assert.True(File.Exists(path));

        var readCursor = CursorStateStore.Read(path);
        Assert.NotNull(readCursor);
        Assert.Equal("id", readCursor.Column);
        Assert.Equal("12345", readCursor.Value);
        Assert.Equal(CursorType.Integer, readCursor.Type);
    }

    [Fact]
    public void Save_OverwritesExisting()
    {
        var path = GetTempPath("overwrite.sync");
        var cursor1 = new CursorValue("val", "foo", CursorType.String);
        var meta1 = new CursorRunMetadata(DateTime.UtcNow, DateTime.UtcNow, 1, "success");
        CursorStateStore.Save(path, cursor1, meta1);

        var cursor2 = new CursorValue("val", "bar", CursorType.String);
        var meta2 = new CursorRunMetadata(DateTime.UtcNow, DateTime.UtcNow, 2, "success");
        CursorStateStore.Save(path, cursor2, meta2);

        var readCursor = CursorStateStore.Read(path);
        Assert.NotNull(readCursor);
        Assert.Equal("val", readCursor.Column);
        Assert.Equal("bar", readCursor.Value);
        Assert.Equal(CursorType.String, readCursor.Type);
    }

    [Fact]
    public void Read_InvalidJson_ReturnsNull()
    {
        var path = GetTempPath("invalid.sync");
        File.WriteAllText(path, "{ invalid json }");

        var result = CursorStateStore.Read(path);
        Assert.Null(result);
    }

    [Fact]
    public void Read_V1Format_ParsesCorrectly()
    {
        var path = GetTempPath("v1.sync");
        var json = @"{
  ""version"": 1,
  ""cursor"": {
    ""column"": ""updated_at"",
    ""value"": ""2026-06-15T23:59:59.000"",
    ""type"": ""datetime""
  },
  ""last_run"": {
    ""started_at"": ""2026-06-16T02:00:00Z"",
    ""completed_at"": ""2026-06-16T02:03:42Z"",
    ""rows_transferred"": 1234,
    ""status"": ""success""
  }
}";
        File.WriteAllText(path, json);

        var result = CursorStateStore.Read(path);
        Assert.NotNull(result);
        Assert.Equal("updated_at", result.Column);
        Assert.Equal("2026-06-15T23:59:59.000", result.Value);
        Assert.Equal(CursorType.DateTime, result.Type);
    }
}
