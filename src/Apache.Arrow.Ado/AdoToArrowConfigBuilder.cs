using System;
using System.Collections.Generic;
using System.Data.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Ado;

/// <summary>
/// Builder for <see cref="AdoToArrowConfig"/>.
/// </summary>
public sealed class AdoToArrowConfigBuilder
{
    private static readonly Apache.Arrow.Serialization.Mapping.ArrowTypeResult TimestampUtc =
        new(new TimestampType(TimeUnit.Microsecond, "UTC"));

    private int _targetBatchSize = AdoToArrowConfig.DefaultTargetBatchSize;
    private bool _includeMetadata = false;
    private Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult>? _typeResolver;

    // Pre-populated with well-known timezone-aware DataTypeName patterns.
    // Keys are matched case-insensitively against DbColumn.DataTypeName before the TypeResolver is called.
    // Use AddDataTypeNameOverride / ClearDataTypeNameOverrides to customise.
    private readonly Dictionary<string, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> _dataTypeNameOverrides =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["timestamptz"]                    = TimestampUtc,  // PostgreSQL shorthand
            ["timestamp with time zone"]       = TimestampUtc,  // standard SQL / PostgreSQL longhand
            ["timestamp with local time zone"] = TimestampUtc,  // Oracle
        };

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
    /// If not set, <see cref="AdoToArrowUtils.GetLogicalTypeFromDbColumn"/> is used.
    /// DataTypeName overrides (see <see cref="AddDataTypeNameOverride"/>) are applied before this resolver.
    /// </summary>
    public AdoToArrowConfigBuilder SetTypeResolver(Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> typeResolver)
    {
        _typeResolver = typeResolver ?? throw new ArgumentNullException(nameof(typeResolver));
        return this;
    }

    /// <summary>
    /// Registers an exact-match (case-insensitive) override for a specific <see cref="DbColumn.DataTypeName"/>.
    /// The override is applied before <see cref="SetTypeResolver"/> for columns whose
    /// <c>DataTypeName</c> matches <paramref name="dataTypeName"/>.
    /// Replaces any existing entry for the same name, including the built-in defaults.
    /// </summary>
    public AdoToArrowConfigBuilder AddDataTypeNameOverride(
        string dataTypeName,
        Apache.Arrow.Serialization.Mapping.ArrowTypeResult result)
    {
        if (string.IsNullOrEmpty(dataTypeName)) throw new ArgumentNullException(nameof(dataTypeName));
        _dataTypeNameOverrides[dataTypeName] = result;
        return this;
    }

    /// <summary>
    /// Removes all DataTypeName overrides, including the built-in defaults
    /// (e.g. <c>timestamptz</c>, <c>timestamp with time zone</c>).
    /// After calling this, <see cref="SetTypeResolver"/> handles every column unconditionally.
    /// </summary>
    public AdoToArrowConfigBuilder ClearDataTypeNameOverrides()
    {
        _dataTypeNameOverrides.Clear();
        return this;
    }

    /// <summary>
    /// Builds and returns the <see cref="AdoToArrowConfig"/>.
    /// </summary>
    public AdoToArrowConfig Build()
    {
        var overrides = new Dictionary<string, Apache.Arrow.Serialization.Mapping.ArrowTypeResult>(
            _dataTypeNameOverrides, StringComparer.OrdinalIgnoreCase);

        var baseResolver = _typeResolver ?? AdoToArrowUtils.GetLogicalTypeFromDbColumn;

        Func<DbColumn, Apache.Arrow.Serialization.Mapping.ArrowTypeResult> composedResolver =
            overrides.Count == 0
                ? baseResolver
                : col =>
                {
                    if (!string.IsNullOrEmpty(col.DataTypeName) &&
                        overrides.TryGetValue(col.DataTypeName, out var overrideResult))
                        return overrideResult;
                    return baseResolver(col);
                };

        return new AdoToArrowConfig(_targetBatchSize, _includeMetadata, composedResolver, overrides);
    }
}
