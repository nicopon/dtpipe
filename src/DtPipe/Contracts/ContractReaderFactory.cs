using System.Runtime.CompilerServices;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;

namespace DtPipe.Contracts;

/// <summary>Options placeholder — a contract source carries no adapter configuration.</summary>
public sealed class ContractReaderOptions : IOptionSet
{
    public static string Prefix => "contract";
    public static string DisplayName => "Contract Source";
}

/// <summary>
/// Makes a saved contract a pipeline source that publishes a schema and no rows.
///
/// <para>
/// Selected by capability, from <c>--from-contract</c>, and never through
/// <c>ComponentSelector</c> — the same rule as <see cref="DtPipe.Sessions.CheckpointReaderFactory"/>
/// and for the same reason: a file path must not enter the <c>{component}[+{variant}]:</c> grammar,
/// where <c>C:\contracts\orders.json</c> is a Windows drive letter followed by a component name.
/// </para>
/// </summary>
public sealed class ContractReaderFactory : IStreamReaderFactory
{
    private readonly string _contractPath;

    public ContractReaderFactory(string contractPath) => _contractPath = contractPath;

    public string ComponentName => "contract";
    public string Category => "Contract";
    public Type OptionsType => typeof(ContractReaderOptions);
    public bool RequiresQuery => false;

    /// <summary>
    /// Always false. The router selects this factory from the flag; nothing may claim the path by
    /// inspecting it.
    /// </summary>
    public bool CanHandle(string connectionString) => false;

    public IEnumerable<Type> GetSupportedOptionTypes() => [typeof(ContractReaderOptions)];

    public IStreamReader Create(OptionsRegistry registry)
    {
        if (!File.Exists(_contractPath))
            throw new InvalidOperationException(
                $"No contract at '{_contractPath}'. The producer writes one with --contract-save.");

        return new ContractStreamReader(DataContract.Read(_contractPath));
    }
}

/// <summary>
/// Publishes a contract's schema, then ends the stream without a single batch.
/// </summary>
/// <remarks>
/// A consumer checked against this runs its whole initialisation — every transformer's
/// <c>InitializeAsync</c>, the segmentation, the target inspection — over the shape the producer
/// promised, and writes nothing. What it cannot exercise is anything that only fails on a row: a
/// <c>--compute</c> reading a column that was removed throws on the first row, not at
/// initialisation. That is the boundary of what a zero-row check decides, and it is why the
/// consumer's report must not be read as "this pipeline will run".
/// </remarks>
public sealed class ContractStreamReader : IColumnarStreamReader
{
    private readonly DataContract _contract;

    public ContractStreamReader(DataContract contract) => _contract = contract;

    public Schema? Schema { get; private set; }
    public IReadOnlyList<PipeColumnInfo>? Columns { get; private set; }

    /// <summary>The contract this reader publishes, for a caller that reports on it.</summary>
    public DataContract Contract => _contract;

    public Task OpenAsync(CancellationToken ct = default)
    {
        Schema = _contract.ToArrowSchema();
        Columns = ArrowSchemaFactory.ToPipeColumns(Schema);
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<RecordBatch> ReadRecordBatchesAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    public async IAsyncEnumerable<ReadOnlyMemory<object?[]>> ReadBatchesAsync(
        int batchSize, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
