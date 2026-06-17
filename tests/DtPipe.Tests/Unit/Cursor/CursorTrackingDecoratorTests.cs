using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Cursor;
using Xunit;

namespace DtPipe.Tests.Unit.Cursor;

public class FakeRowDataWriter : IRowDataWriter
{
    public bool InitializeCalled { get; private set; }
    public bool WriteBatchCalled { get; private set; }
    public bool CompleteCalled { get; private set; }
    public bool DisposeCalled { get; private set; }

    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }
    public List<object?[]> Batches { get; } = new();

    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default)
    {
        InitializeCalled = true;
        Columns = columns;
        return ValueTask.CompletedTask;
    }

    public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        WriteBatchCalled = true;
        Batches.AddRange(rows);
        return ValueTask.CompletedTask;
    }

    public ValueTask CompleteAsync(CancellationToken ct = default)
    {
        CompleteCalled = true;
        return ValueTask.CompletedTask;
    }

    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        DisposeCalled = true;
        return ValueTask.CompletedTask;
    }
}

public class CursorTrackingDecoratorTests
{
    [Fact]
    public async Task InitializeAsync_ColumnExists_Succeeds()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "updated_at");

        var columns = new List<PipeColumnInfo>
        {
            new PipeColumnInfo("id", typeof(int), false),
            new PipeColumnInfo("updated_at", typeof(DateTime), false)
        };

        await decorator.InitializeAsync(columns);

        Assert.True(inner.InitializeCalled);
        Assert.Null(decorator.TrackedMaxValue);
    }

    [Fact]
    public async Task InitializeAsync_ColumnMissing_Throws()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "updated_at");

        var columns = new List<PipeColumnInfo>
        {
            new PipeColumnInfo("id", typeof(int), false)
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => decorator.InitializeAsync(columns).AsTask());
    }

    [Fact]
    public async Task InitializeAsync_ColumnCaseInsensitive_Succeeds()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "updated_at");

        var columns = new List<PipeColumnInfo>
        {
            new PipeColumnInfo("id", typeof(int), false),
            new PipeColumnInfo("Updated_At", typeof(DateTime), false)
        };

        await decorator.InitializeAsync(columns);

        Assert.True(inner.InitializeCalled);
    }

    [Fact]
    public async Task WriteBatchAsync_TracksMaxDateTime()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "updated_at");

        var columns = new List<PipeColumnInfo>
        {
            new PipeColumnInfo("id", typeof(int), false),
            new PipeColumnInfo("updated_at", typeof(DateTime), false)
        };

        await decorator.InitializeAsync(columns);

        var baseTime = new DateTime(2026, 6, 16, 12, 0, 0, DateTimeKind.Utc);

        var batch1 = new List<object?[]>
        {
            new object?[] { 1, baseTime },
            new object?[] { 2, baseTime.AddHours(2) }
        };

        var batch2 = new List<object?[]>
        {
            new object?[] { 3, baseTime.AddHours(1) },
            new object?[] { 4, baseTime.AddHours(5) }
        };

        await decorator.WriteBatchAsync(batch1);
        await decorator.WriteBatchAsync(batch2);

        var max = decorator.TrackedMaxValue;
        Assert.NotNull(max);
        Assert.Equal("updated_at", max.Column);
        Assert.Equal(CursorType.DateTime, max.Type);
        Assert.Equal(baseTime.AddHours(5).ToString("yyyy-MM-ddTHH:mm:ss.fff"), max.Value);
    }

    [Fact]
    public async Task WriteBatchAsync_TracksMaxInt()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "id");

        var columns = new List<PipeColumnInfo>
        {
            new PipeColumnInfo("id", typeof(int), false),
            new PipeColumnInfo("name", typeof(string), true)
        };

        await decorator.InitializeAsync(columns);

        var batch1 = new List<object?[]>
        {
            new object?[] { 10, "Alice" },
            new object?[] { 42, "Bob" }
        };

        var batch2 = new List<object?[]>
        {
            new object?[] { 5, "Charlie" }
        };

        await decorator.WriteBatchAsync(batch1);
        await decorator.WriteBatchAsync(batch2);

        var max = decorator.TrackedMaxValue;
        Assert.NotNull(max);
        Assert.Equal("id", max.Column);
        Assert.Equal(CursorType.Integer, max.Type);
        Assert.Equal("42", max.Value);
    }

    [Fact]
    public async Task WriteBatchAsync_IgnoresNullValues()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "id");

        var columns = new List<PipeColumnInfo>
        {
            new PipeColumnInfo("id", typeof(int?), true)
        };

        await decorator.InitializeAsync(columns);

        var batch = new List<object?[]>
        {
            new object?[] { null },
            new object?[] { 10 },
            new object?[] { DBNull.Value }
        };

        await decorator.WriteBatchAsync(batch);

        var max = decorator.TrackedMaxValue;
        Assert.NotNull(max);
        Assert.Equal("10", max.Value);
    }

    [Fact]
    public async Task WriteBatchAsync_EmptyBatch_NoChange()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "id");

        var columns = new List<PipeColumnInfo>
        {
            new PipeColumnInfo("id", typeof(int), false)
        };

        await decorator.InitializeAsync(columns);
        await decorator.WriteBatchAsync(new List<object?[]>());

        Assert.Null(decorator.TrackedMaxValue);
    }

    [Fact]
    public async Task WriteBatchAsync_DelegatesToInner()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "id");

        var columns = new List<PipeColumnInfo>
        {
            new PipeColumnInfo("id", typeof(int), false)
        };

        await decorator.InitializeAsync(columns);

        var batch = new List<object?[]>
        {
            new object?[] { 1 }
        };

        await decorator.WriteBatchAsync(batch);

        Assert.True(inner.WriteBatchCalled);
        Assert.Single(inner.Batches);
        Assert.Equal(1, inner.Batches[0][0]);
    }

    [Fact]
    public async Task CompleteAsync_DelegatesToInner()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "id");

        await decorator.CompleteAsync();

        Assert.True(inner.CompleteCalled);
    }

    [Fact]
    public void TrackedMaxValue_IsNull_BeforeAnyWrite()
    {
        var inner = new FakeRowDataWriter();
        var decorator = new CursorTrackingRowDecorator(inner, "id");

        Assert.Null(decorator.TrackedMaxValue);
    }
}
