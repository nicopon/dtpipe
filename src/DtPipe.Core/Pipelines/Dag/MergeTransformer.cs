using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// An <see cref="IStreamTransformer"/> that concatenates the output of two upstream Arrow channels.
/// The main channel is declared via <c>--from &lt;alias&gt;</c> and the secondary via
/// <c>--merge &lt;alias&gt;</c>. Both channels must carry Arrow RecordBatches.
///
/// All batches from the main channel are emitted first, then all batches from the merge channel.
/// </summary>
public sealed class MergeTransformer : IStreamTransformer
{
    private readonly IArrowChannelRegistry _registry;
    private readonly string _mainAlias;
    private readonly string _mergeAlias;
    private IReadOnlyList<PipeColumnInfo>? _columns;

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;

    public MergeTransformer(IArrowChannelRegistry registry, string mainAlias, string mergeAlias)
    {
        _registry = registry;
        _mainAlias = mainAlias;
        _mergeAlias = mergeAlias;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        // Wait for both channels to publish their schemas.
        var mainSchema = await _registry.WaitForArrowChannelSchemaAsync(_mainAlias, ct);
        await _registry.WaitForArrowChannelSchemaAsync(_mergeAlias, ct);

        _columns = mainSchema.FieldsList
            .Select(f => new PipeColumnInfo(f.Name, ArrowTypeMapper.GetClrType(f.DataType), f.IsNullable))
            .ToList();
    }

    public async IAsyncEnumerable<RecordBatch> ReadResultsAsync(
        IAsyncEnumerable<RecordBatch>? inputStream = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var mainChannel = _registry.GetArrowChannel(_mainAlias)
            ?? throw new InvalidOperationException($"MergeTransformer: main channel '{_mainAlias}' not found.");
        var mergeChannel = _registry.GetArrowChannel(_mergeAlias)
            ?? throw new InvalidOperationException($"MergeTransformer: merge channel '{_mergeAlias}' not found.");

        await foreach (var batch in mainChannel.Channel.Reader.ReadAllAsync(ct))
            yield return batch;

        await foreach (var batch in mergeChannel.Channel.Reader.ReadAllAsync(ct))
            yield return batch;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
