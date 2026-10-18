using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
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
/// Schema is taken from the first channel; every other channel must declare the same columns
/// (count, names, structural types), or <see cref="OpenAsync"/> refuses the merge: nothing is
/// converted to make them agree.
/// </summary>
public sealed class MergeTransformer : IStreamTransformer
{
    private readonly IMemoryChannelRegistry _registry;
    private readonly IReadOnlyList<string> _aliases;
    private IReadOnlyList<PipeColumnInfo>? _columns;
    private Schema? _schema;

    public IReadOnlyList<PipeColumnInfo>? Columns => _columns;
    public Schema? Schema => _schema;

    public MergeTransformer(IMemoryChannelRegistry registry, IReadOnlyList<string> aliases)
    {
        if (aliases.Count < 2)
            throw new ArgumentException("MergeTransformer requires at least 2 channel aliases.", nameof(aliases));
        _registry = registry;
        _aliases = aliases;
    }

    public async Task OpenAsync(CancellationToken ct = default)
    {
        // Wait for all channels to publish their schemas; derive column metadata from the first.
        var schemas = new List<Schema>(_aliases.Count);
        foreach (var alias in _aliases)
            schemas.Add(await _registry.WaitForArrowChannelSchemaAsync(alias, ct));

        for (var i = 1; i < schemas.Count; i++)
            RequireSameColumns(_aliases[0], schemas[0], _aliases[i], schemas[i]);

        _schema = schemas[0];
        _columns = ArrowSchemaFactory.ToPipeColumns(_schema).ToList();
    }

    // A published channel schema is built from the branch's columns: it carries no zone and no
    // extension metadata, so the comparison is on names and structural types only.
    private static void RequireSameColumns(string firstAlias, Schema first, string alias, Schema other)
    {
        var expected = first.FieldsList;
        var actual = other.FieldsList;
        if (expected.Count != actual.Count)
            throw Mismatch(firstAlias, alias,
                $"'{alias}' has {actual.Count} columns where '{firstAlias}' has {expected.Count}");

        for (var i = 0; i < expected.Count; i++)
        {
            if (expected[i].Name != actual[i].Name)
                throw Mismatch(firstAlias, alias,
                    $"column {i} is '{expected[i].Name}' in '{firstAlias}' and '{actual[i].Name}' in '{alias}'");

            var expectedType = TypeKey(expected[i].DataType);
            var actualType = TypeKey(actual[i].DataType);
            if (expectedType != actualType)
                throw Mismatch(firstAlias, alias,
                    $"column '{expected[i].Name}' is {expectedType} in '{firstAlias}' and {actualType} in '{alias}'");
        }
    }

    private static string TypeKey(IArrowType type) => type switch
    {
        TimestampType t => $"timestamp[{t.Unit.ToString().ToLowerInvariant()}]",
        NestedType n => $"{n.Name}<{string.Join(", ", n.Fields.Select(f => $"{f.Name}: {TypeKey(f.DataType)}"))}>",
        _ => type.Name,
    };

    private static InvalidOperationException Mismatch(string firstAlias, string alias, string detail)
        => new($"--merge: channels '{firstAlias}' and '{alias}' do not share a schema: {detail}. "
             + "Align the branches (--column-type, --compute or --sql); nothing is converted.");

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
