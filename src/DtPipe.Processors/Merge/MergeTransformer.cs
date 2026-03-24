using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Processors.Merge;

/// <summary>
/// An <see cref="IStreamTransformer"/> that concatenates (UNION ALL) the output of N upstream
/// Arrow channels. Channels are declared via <c>--from a,b,c</c> (comma-separated) and
/// activated by the boolean flag <c>--merge</c>. All channels must carry Arrow RecordBatches.
///
/// Batches are emitted sequentially in the order the aliases were declared.
/// Schema is taken from the first channel; all channels must share the same schema.
/// </summary>
public sealed class MergeTransformer : IStreamTransformer
{
    private readonly IArrowChannelRegistry _registry;
    private readonly IReadOnlyList<string> _aliases;
    private IReadOnlyList<PipeColumnInfo>? _columns;

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;

    public MergeTransformer(IArrowChannelRegistry registry, IReadOnlyList<string> aliases)
    {
        if (aliases.Count < 2)
            throw new ArgumentException("MergeTransformer requires at least 2 channel aliases.", nameof(aliases));
        _registry = registry;
        _aliases = aliases;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        // Wait for all channels to publish their schemas; derive column metadata from the first.
        foreach (var alias in _aliases)
            await _registry.WaitForArrowChannelSchemaAsync(alias, ct);

        var firstSchema = await _registry.WaitForArrowChannelSchemaAsync(_aliases[0], ct);
        _columns = firstSchema.FieldsList
            .Select(f => new PipeColumnInfo(f.Name, ArrowTypeMapper.GetClrType(f.DataType), f.IsNullable))
            .ToList();
    }

    public async IAsyncEnumerable<RecordBatch> ReadResultsAsync(
        IAsyncEnumerable<RecordBatch>? inputStream = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var alias in _aliases)
        {
            var channel = _registry.GetArrowChannel(alias)
                ?? throw new InvalidOperationException($"MergeTransformer: channel '{alias}' not found.");
            await foreach (var batch in channel.Channel.Reader.ReadAllAsync(ct))
                yield return batch;
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
