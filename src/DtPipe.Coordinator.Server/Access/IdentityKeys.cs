using System.Buffers.Text;
using System.Security.Cryptography;
using System.Text;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server.Access;

/// <summary>
/// How the coordinator knows a caller is an identity: the trusted IdP says so (<see cref="AuthModes.Idp"/>, and the identity holds
/// no key), or the caller holds one of <see cref="Keys"/> (<see cref="AuthModes.Key"/>). Chosen when the identity is created, never
/// changed by an edit. An identity written without one reads as <see cref="Idp"/>.
/// </summary>
public sealed record IdentityAuth(string Mode, IReadOnlyList<IdentityKey> Keys)
{
    public static readonly IdentityAuth Idp = new(AuthModes.Idp, []);

    /// <summary>The keys that open the identity at <paramref name="now"/>: neither revoked nor expired.</summary>
    public IEnumerable<IdentityKey> LiveAt(DateTimeOffset now) => Keys.Where(k => k.LiveAt(now));
}

/// <summary>What verifies an identity.</summary>
public static class AuthModes
{
    /// <summary>The trusted IdP attests it; the identity has no key.</summary>
    public const string Idp = "idp";

    /// <summary>A key the coordinator made for it.</summary>
    public const string Key = "key";

    public static readonly IReadOnlyList<string> All = [Idp, Key];
}

/// <summary>
/// One key of an identity as the rights keep it: the SHA-256 of the key, never the key, which exists in clear only in the answer that
/// made it. <see cref="Hint"/> is its last four characters, so a person tells two keys apart. A revoked key stays, with its date, and
/// opens nothing. <see cref="ToString"/> leaves the hash out, and no route answers it.
/// </summary>
public sealed record IdentityKey(
    string Id, string Hash, string Hint, DateTimeOffset Created, string? Label = null, DateTimeOffset? Expires = null, DateTimeOffset? Revoked = null)
{
    public bool LiveAt(DateTimeOffset now) => Revoked is null && (Expires is null || Expires > now);

    public override string ToString() => $"key {Id} (…{Hint})";
}

/// <summary>
/// An identity key in clear, <c>dtp_{clientId}_{secret}</c>: 256 random bits as 43 characters of base64url, after the client id of the
/// identity it opens. The shape is the studio's contract (<see cref="CoordinatorApi.ClientIdOfKey"/>), since the studio presents a key
/// too. The client id only finds the identity: the key is then judged by its hash alone. <see cref="Reveal"/> is the one way to the
/// text, and <see cref="ToString"/> shows none of it, so a key handed to a log by mistake leaks nothing.
/// </summary>
public sealed class ClearKey
{
    public const string Prefix = CoordinatorApi.KeyPrefix;
    public const int SecretLength = CoordinatorApi.KeySecretLength;
    private const int SecretBytes = 32;
    private const int HintLength = 4;

    private readonly string _text;

    private ClearKey(string text) => _text = text;

    public static ClearKey New(string clientId) =>
        new($"{Prefix}{clientId}_{Base64Url.EncodeToString(RandomNumberGenerator.GetBytes(SecretBytes))}");

    /// <summary>
    /// The key <paramref name="text"/> is, or null when it does not have a key's shape: a client id, then 43 characters of base64url. For a
    /// key the deployment gives the server (<see cref="RightsBootstrap"/>), never one a caller presents, which <see cref="RightsStore.KeyOf"/> judges.
    /// </summary>
    public static ClearKey? From(string? text) =>
        ClientIdOf(text) is not null && text![^SecretLength..].All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_') ? new ClearKey(text) : null;

    /// <summary>The client id of the identity the key opens.</summary>
    public string ClientId => ClientIdOf(_text)!;

    /// <summary>The key's text, for the one answer that hands it over.</summary>
    public string Reveal() => _text;

    public string Hash => HashOf(_text);

    public string Hint => _text[^HintLength..];

    public override string ToString() => Prefix + "***";

    /// <summary>The client id <paramref name="presented"/> names, or null when it does not have a key's shape.</summary>
    public static string? ClientIdOf(string? presented) => CoordinatorApi.ClientIdOfKey(presented);

    /// <summary>Whether <paramref name="presented"/> is the key whose SHA-256 is <paramref name="hash"/>, in a time that does not depend on where they differ.</summary>
    public static bool Matches(string presented, string hash)
    {
        byte[] expected;
        try { expected = Convert.FromHexString(hash); }
        catch (FormatException) { return false; }
        return CryptographicOperations.FixedTimeEquals(SHA256.HashData(Encoding.UTF8.GetBytes(presented)), expected);
    }

    private static string HashOf(string text) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
}
