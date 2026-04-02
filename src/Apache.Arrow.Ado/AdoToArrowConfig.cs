using System;
using System.Data.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// Configuration for the ADO.NET to Arrow conversion process.
/// </summary>
public sealed class AdoToArrowConfig
{
    public const int DefaultTargetBatchSize = 1024;

    /// <summary>
    /// Gets the target size (number of rows) for each RecordBatch.
    /// The actual batch size might be smaller for the last batch.
    /// </summary>
    public int TargetBatchSize { get; }

    /// <summary>
    /// Gets whether to include DB column metadata in the Arrow schema.
    /// </summary>
    public bool IncludeMetadata { get; }

    /// <summary>
    /// Gets the function that maps a <see cref="DbColumn"/> to an Arrow type.
    /// Defaults to <see cref="AdoToArrowUtils.GetArrowTypeFromDbColumn"/>.
    /// Inject a custom resolver to align the Arrow schema with your type system
    /// (e.g. to use ArrowTypeMapper from DtPipe.Core for pipeline consistency).
    /// </summary>
    public Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> TypeResolver { get; }

    internal AdoToArrowConfig(int targetBatchSize, bool includeMetadata, Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> typeResolver)
    {
        TargetBatchSize = targetBatchSize;
        IncludeMetadata = includeMetadata;
        TypeResolver = typeResolver;
    }
}
