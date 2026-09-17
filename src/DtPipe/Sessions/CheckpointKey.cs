using System.Security.Cryptography;
using System.Text;
using DtPipe.Core.Security;

namespace DtPipe.Sessions;

/// <summary>
/// Names a checkpoint by <b>what produces it</b>, not by what it was called.
///
/// The key hashes the definition of the branch prefix — connection, query, the transformers up
/// to the point, and the sampling parameters. Three things follow, and they are why the
/// implicit working-directory session is safe:
///
/// <list type="bullet">
/// <item>Two different pipelines launched in the same directory can never collide. By
/// construction, not by the user remembering to name them apart.</item>
/// <item>An unchanged prefix is reused for free — which is what makes an agent's
/// propose/observe/correct loop fast.</item>
/// <item>The key covers the DEFINITION, never the DATA. A checkpoint is a snapshot, and the
/// snapshot semantics stay explicit rather than being smuggled in as cache invalidation.</item>
/// </list>
///
/// Renaming an alias does not change the key: an alias is a label, and labels are not identity.
/// </summary>
public static class CheckpointKey
{
    /// <summary>Hex length of a key. Half a SHA-256, which is far past collision concerns here.</summary>
    public const int Length = 32;

    /// <summary>
    /// Computes the key for a branch prefix.
    /// </summary>
    /// <param name="connection">The reader's connection string. Redacted before hashing —
    /// a credential must never end up inside a cache key, where it would sit in a directory
    /// name and in every listing that prints one. It goes through the same entry point as every
    /// other connection string the product handles, so this site is not an exception a grep has
    /// to learn about.</param>
    public static string Compute(
        string? connection,
        string? query,
        IEnumerable<string>? transformerDefinitions,
        double samplingRate,
        int? samplingSeed,
        int limit,
        int batchSize,
        long maxBatchBytes,
        int segmentIndex)
    {
        var sb = new StringBuilder();
        Append(sb, "conn", ConnectionStringSanitizer.Redact(connection ?? ""));
        Append(sb, "query", query ?? "");

        var index = 0;
        foreach (var t in transformerDefinitions ?? Enumerable.Empty<string>())
            Append(sb, $"t{index++}", t);

        Append(sb, "rate", samplingRate.ToString("R", System.Globalization.CultureInfo.InvariantCulture));
        Append(sb, "seed", samplingSeed?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "");
        Append(sb, "limit", limit.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(sb, "batch", batchSize.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(sb, "bytes", maxBatchBytes.ToString(System.Globalization.CultureInfo.InvariantCulture));
        Append(sb, "segment", segmentIndex.ToString(System.Globalization.CultureInfo.InvariantCulture));

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()));
        return Convert.ToHexStringLower(hash)[..Length];
    }

    /// <summary>
    /// Length-prefixes each part so no two different definitions can serialise to the same
    /// string. Without it, ("ab", "c") and ("a", "bc") would hash alike.
    /// </summary>
    private static void Append(StringBuilder sb, string name, string value)
        => sb.Append(name).Append(':').Append(value.Length).Append(':').Append(value).Append('\n');
}
