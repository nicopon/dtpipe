using System;
using System.Text;
using DtPipe.Adapters.Common;

namespace DtPipe.Adapters.ObjectStorage;

/// <summary>
/// Everything needed to reach one object-storage location through the in-process DuckDB engine:
/// where the bytes are, how to read/write that format, and the credential setup to run first.
///
/// Object storage is treated as a transport, so no format logic is duplicated here — DuckDB's
/// httpfs/azure extensions stream the bytes and the same Arrow path as any other DuckDB source
/// carries them into the pipeline. That also keeps globbing, range requests and multipart
/// uploads working, none of which a download-then-parse design would provide.
/// </summary>
public sealed class ObjectStorageBinding
{
    public required ObjectUri Uri { get; init; }
    public required ObjectFormatMap.FormatSpec Format { get; init; }
    public required SecretSql Secret { get; init; }

    /// <summary>DuckDB extension backing this scheme ("httpfs" for S3, "azure" for Azure Blob).</summary>
    public required string SchemeExtension { get; init; }

    /// <summary>SQL run once the connection opens: extension load, then the scoped secret.</summary>
    public string InitSql
    {
        get
        {
            var sb = new StringBuilder();
            // Only the scheme needs an extension: parquet/csv/json readers are DuckDB core.
            sb.Append("INSTALL ").Append(SchemeExtension).Append("; LOAD ").Append(SchemeExtension).Append("; ");
            sb.Append(Secret.Sql);
            return sb.ToString();
        }
    }

    /// <summary>Read query for this location. Globs are handled natively by the read function.</summary>
    public string SelectQuery => $"SELECT * FROM {Format.ReadFunction}('{Escape(Uri.DuckDbUri)}')";

    /// <summary>COPY statement writing <paramref name="quotedSourceTable"/> out to this location.</summary>
    public string BuildCopyStatement(string quotedSourceTable)
        => $"COPY (SELECT * FROM {quotedSourceTable}) TO '{Escape(Uri.DuckDbUri)}' (FORMAT {Format.CopyFormat});";

    private static string Escape(string value) => value.Replace("'", "''", StringComparison.Ordinal);
}
