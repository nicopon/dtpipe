using System;
using System.Collections.Generic;

namespace DtPipe.Adapters.Common;

/// <summary>
/// A parsed object-storage location ("s3://bucket/prefix/key.parquet").
///
/// Object storage is a transport for files, not a relational catalog: the location identifies
/// bytes, and the file extension identifies the format. Parsing is deliberately strict — an
/// unrecognised shape fails closed rather than being guessed at, mirroring the scheme rule in
/// <see cref="ConnectionUri.HasRemoteScheme"/>.
/// </summary>
public sealed class ObjectUri
{
    /// <summary>Scheme exactly as the user typed it ("s3", "s3a", "azure", "az").</summary>
    public required string Scheme { get; init; }

    /// <summary>Bucket (S3) or container (Azure) — the first path segment after the scheme.</summary>
    public required string Container { get; init; }

    /// <summary>Everything after the container, without a leading slash. May contain glob wildcards.</summary>
    public required string Key { get; init; }

    /// <summary>Lower-case extension of the last key segment, including the dot ("" when absent).</summary>
    public required string Extension { get; init; }

    /// <summary>
    /// The URI in the form DuckDB's httpfs/azure extensions expect. The alternate spellings
    /// ("s3a://", "az://") are accepted from users but never emitted into SQL, because the
    /// extensions only register the canonical scheme.
    /// </summary>
    public string DuckDbUri => $"{CanonicalScheme}://{Container}/{Key}";

    /// <summary>Canonical scheme for the underlying DuckDB extension ("s3" or "azure").</summary>
    public string CanonicalScheme => Scheme.ToLowerInvariant() switch
    {
        "s3" or "s3a" => "s3",
        "azure" or "az" => "azure",
        var other => other,
    };

    /// <summary>Narrowest scope a DuckDB secret can be bound to for this location.</summary>
    public string SecretScope => $"{CanonicalScheme}://{Container}";

    public static bool TryParse(string connectionString, IReadOnlySet<string> acceptedSchemes, out ObjectUri? uri)
    {
        uri = null;
        if (!ConnectionUri.HasRemoteScheme(connectionString)) return false;

        var schemeEnd = connectionString.IndexOf("://", StringComparison.Ordinal);
        var scheme = connectionString.Substring(0, schemeEnd);
        if (!acceptedSchemes.Contains(scheme)) return false;

        var remainder = connectionString.Substring(schemeEnd + 3);
        var slash = remainder.IndexOf('/');
        // A container on its own is not a location: there are no bytes to read or write.
        if (slash <= 0 || slash == remainder.Length - 1) return false;

        var container = remainder.Substring(0, slash);
        var key = remainder.Substring(slash + 1);

        var lastSegment = key.Substring(key.LastIndexOf('/') + 1);
        var dot = lastSegment.LastIndexOf('.');
        var extension = dot >= 0 ? lastSegment.Substring(dot).ToLowerInvariant() : string.Empty;

        uri = new ObjectUri
        {
            Scheme = scheme,
            Container = container,
            Key = key,
            Extension = extension,
        };
        return true;
    }

    public static ObjectUri Parse(string connectionString, IReadOnlySet<string> acceptedSchemes)
    {
        if (!TryParse(connectionString, acceptedSchemes, out var uri) || uri is null)
        {
            throw new InvalidOperationException(
                $"'{DtPipe.Core.Security.ConnectionStringSanitizer.Redact(connectionString)}' is not a valid object-storage location. " +
                $"Expected <scheme>://<container>/<key>, with scheme one of: {string.Join(", ", acceptedSchemes)}.");
        }
        return uri;
    }
}
