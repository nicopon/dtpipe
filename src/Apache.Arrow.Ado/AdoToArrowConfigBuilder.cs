using System;
using System.Data.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// Builder for <see cref="AdoToArrowConfig"/>.
/// </summary>
public sealed class AdoToArrowConfigBuilder
{
    private int _targetBatchSize = AdoToArrowConfig.DefaultTargetBatchSize;
    private bool _includeMetadata = false;
    private Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult>? _typeResolver;

    /// <summary>
    /// Sets the target number of rows per Arrow RecordBatch.
    /// </summary>
    public AdoToArrowConfigBuilder SetTargetBatchSize(int targetBatchSize)
    {
        if (targetBatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(targetBatchSize), "Target batch size must be greater than 0.");
        _targetBatchSize = targetBatchSize;
        return this;
    }

    /// <summary>
    /// Sets whether to include DB column metadata in the resulting Arrow Schema.
    /// </summary>
    public AdoToArrowConfigBuilder SetIncludeMetadata(bool includeMetadata)
    {
        _includeMetadata = includeMetadata;
        return this;
    }

    /// <summary>
    /// Sets a custom type resolver that maps a <see cref="DbColumn"/> to an Arrow type.
    /// Use this to align the produced Arrow schema with an external type system
    /// (e.g. DtPipe.Core's ArrowTypeMapper) without creating a dependency on it.
    /// If not set, <see cref="AdoToArrowUtils.GetArrowTypeFromDbColumn"/> is used.
    /// </summary>
    public AdoToArrowConfigBuilder SetTypeResolver(Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> typeResolver)
    {
        _typeResolver = typeResolver ?? throw new ArgumentNullException(nameof(typeResolver));
        return this;
    }

    /// <summary>
    /// Builds and returns the <see cref="AdoToArrowConfig"/>.
    /// </summary>
    public AdoToArrowConfig Build()
    {
        var resolver = _typeResolver ?? AdoToArrowUtils.GetLogicalTypeFromDbColumn;
        return new AdoToArrowConfig(_targetBatchSize, _includeMetadata, resolver);
    }
}
