using System.Security.Cryptography;
using System.Text;

namespace DtPipe.Coordinator.Contracts;

/// <summary>
/// A text's SHA-256, taken over its UTF-8 bytes and written in lowercase hexadecimal, cut to <c>length</c> characters
/// when a view shows only the start. Two sides that hash the same text read the same value.
/// </summary>
public static class ContentHash
{
    /// <summary>The characters of a whole hash.</summary>
    public const int Full = 64;

    public static string Of(string text, int length = Full) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text)))[..length];

    /// <summary>The HMAC-SHA256 of <paramref name="text"/> under <paramref name="key"/> (both as UTF-8), written as <see cref="Of"/> writes a hash.</summary>
    public static string Keyed(string text, string key, int length = Full) =>
        Convert.ToHexStringLower(HMACSHA256.HashData(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(text)))[..length];
}
