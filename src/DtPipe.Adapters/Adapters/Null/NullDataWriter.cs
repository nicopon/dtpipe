using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;

namespace DtPipe.Adapters.Null;

public class NullDataWriter : IRowDataWriter
{
    public string ComponentName => "null";
    public ValueTask InitializeAsync(IReadOnlyList<PipeColumnInfo> columns, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask WriteBatchAsync(IReadOnlyList<object?[]> batch, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask CompleteAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask ExecuteCommandAsync(string command, CancellationToken ct = default)
        => throw new NotSupportedException("Executing raw commands is not supported for the null writer.");
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
