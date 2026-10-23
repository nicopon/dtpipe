using DtPipe.Core.Options;
namespace DtPipe.Core.Abstractions;

/// <summary>
/// Factory for creating database readers for a specific provider.
/// </summary>
public interface IStreamReaderFactory : IDataFactory
{
    /// <summary>
    /// Creates a reader for the given options registry.
    /// </summary>
    IStreamReader Create(OptionsRegistry registry);

    /// <summary>
    /// Gets the option types supported by this reader.
    /// </summary>
    IEnumerable<Type> GetSupportedOptionTypes();

    /// <summary>
    /// Indicates if this factory requires a SQL query to create a reader.
    /// </summary>
    bool RequiresQuery { get; }

    /// <summary>
    /// If true, the reader created by this factory implements IColumnarStreamReader
    /// and can produce Apache Arrow RecordBatches natively.
    /// When true, the DagOrchestrator will use 'arrow-memory' for this branch's output alias.
    /// Default is false.
    /// </summary>
    bool YieldsColumnarOutput => false;

    /// <summary>
    /// Whether this reader opens an external source of its own. False for a reader fed by what
    /// another stage produced: an internal channel (<see cref="IDataFactory.CapabilityKind"/>), a
    /// stream processor or a stored checkpoint. A sample bound (<c>--dry-run N</c>) and a checkpoint
    /// key's limit apply to a source only: what a non-source reads is already bounded upstream.
    /// </summary>
    bool IsSource => CapabilityKind is null;
}
