using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using System.Threading.Channels;
using Apache.Arrow;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// A scoped proxy for the memory channel registry.
/// It intercepts alias lookups and redirects them based on the current branch execution context.
/// This allows processors to use logical aliases (e.g. "c") even when they are physically
/// connected to a broadcast sub-channel (e.g. "c__fan_0").
/// </summary>
public class MappedMemoryChannelRegistry : IMemoryChannelRegistry
{
    private readonly IGlobalMemoryChannelRegistry _inner;
    private readonly BranchExecutionContext _context;
    public MappedMemoryChannelRegistry(IGlobalMemoryChannelRegistry inner, BranchExecutionContext context)
    {
        _inner = inner;
        _context = context;
    }

    private string Resolve(string alias) 
        => _context.AliasMap.TryGetValue(alias, out var physical) ? physical : alias;

    public void RegisterChannel(string branchAlias, Channel<IReadOnlyList<object?[]>> channel, IReadOnlyList<PipeColumnInfo> columns)
        => _inner.RegisterChannel(Resolve(branchAlias), channel, columns);

    public void UpdateChannelColumns(string branchAlias, IReadOnlyList<PipeColumnInfo> columns)
        => _inner.UpdateChannelColumns(Resolve(branchAlias), columns);

    public Task<IReadOnlyList<PipeColumnInfo>> WaitForChannelColumnsAsync(string branchAlias, CancellationToken ct = default)
        => _inner.WaitForChannelColumnsAsync(Resolve(branchAlias), ct);

    public (Channel<IReadOnlyList<object?[]>> Channel, IReadOnlyList<PipeColumnInfo> Columns)? GetChannel(string branchAlias)
        => _inner.GetChannel(Resolve(branchAlias));

    public bool ContainsChannel(string branchAlias)
        => _inner.ContainsChannel(Resolve(branchAlias));

    public void RegisterArrowChannel(string branchAlias, Channel<RecordBatch> channel, Schema schema)
        => _inner.RegisterArrowChannel(Resolve(branchAlias), channel, schema);

    public void UpdateArrowChannelSchema(string branchAlias, Schema schema)
        => _inner.UpdateArrowChannelSchema(Resolve(branchAlias), schema);

    public (Channel<RecordBatch> Channel, Schema Schema)? GetArrowChannel(string branchAlias)
        => _inner.GetArrowChannel(Resolve(branchAlias));

    public Task<Schema> WaitForArrowChannelSchemaAsync(string branchAlias, CancellationToken ct = default)
        => _inner.WaitForArrowChannelSchemaAsync(Resolve(branchAlias), ct);
}
