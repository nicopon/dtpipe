using System;
using System.Collections.Generic;
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
    /// Defaults to <see cref="AdoToArrowUtils.GetLogicalTypeFromDbColumn"/>.
    /// Inject a custom resolver to align the Arrow schema with your type system
    /// (e.g. to use ArrowTypeMapper from DtPipe.Core for pipeline consistency).
    /// </summary>
    public Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> TypeResolver { get; }

    /// <summary>
    /// Gets the exact-match, case-insensitive overrides applied before <see cref="TypeResolver"/>.
    /// Keyed on <see cref="System.Data.Common.DbColumn.DataTypeName"/>. Built from
    /// <see cref="AdoToArrowConfigBuilder.AddDataTypeNameOverride"/>.
    /// </summary>
    public IReadOnlyDictionary<string, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> DataTypeNameOverrides { get; }

    internal AdoToArrowConfig(
        int targetBatchSize,
        bool includeMetadata,
        Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> typeResolver,
        IReadOnlyDictionary<string, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> dataTypeNameOverrides)
    {
        TargetBatchSize = targetBatchSize;
        IncludeMetadata = includeMetadata;
        TypeResolver = typeResolver;
        DataTypeNameOverrides = dataTypeNameOverrides;
    }
}
