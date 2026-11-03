using System.Buffers.Text;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

namespace DtPipe.Jobs.Coordinator;

/// <summary>
/// The part of the coordinator's HTTP API that <c>dtpipe studio</c> calls: the contract number, the
/// routes and the rules both sides apply to what crosses them. The server maps these routes and
/// serialises the records of this namespace; the studio builds its requests from them. A change to a
/// route or a record a deployed studio reads takes a new <see cref="Contract"/>.
/// </summary>
public static partial class CoordinatorApi
{
    /// <summary>
    /// The contract the coordinator announces in its metadata document and the studio checks before
    /// anything else: a studio and a coordinator deployed at different times refuse each other by name
    /// rather than misreading each other.
    /// </summary>
    public const int Contract = 10;

    /// <summary>The protected resource metadata (RFC 9728): anonymous, the one entry point a studio needs.</summary>
    public const string Metadata = "/.well-known/oauth-protected-resource";

    /// <summary>The header every request that changes something carries, a bearer's included.</summary>
    public const string CsrfHeader = "X-Dtpipe-Csrf";

    /// <summary>The most rows a brick sample sends.</summary>
    public const int MaxSampleRows = 100;

    /// <summary>The prefix of every route below; the server maps them in groups under it.</summary>
    public const string Prefix = "/api";

    public const string MeRoute = "/me";
    public const string CatalogRoute = "/catalog";
    public const string RecognizeRoute = "/bricks/recognize";
    public const string SampleRoute = "/bricks/{node}/{id}/sample";
    public const string PreviewRoute = "/bricks/{node}/{id}/preview";
    public const string LibraryListRoute = "/library";
    public const string LibraryRoute = "/library/{id}";
    public const string CursorsRoute = "/library/{id}/cursors";
    public const string CursorRoute = "/library/{id}/cursors/{alias}";

    public static string Me => Prefix + MeRoute;
    public static string Catalog => Prefix + CatalogRoute;
    public static string Recognize => Prefix + RecognizeRoute;

    public static string Sample(string node, string id) =>
        Prefix + SampleRoute.Replace("{node}", Uri.EscapeDataString(node)).Replace("{id}", Uri.EscapeDataString(id));

    public static string Preview(string node, string id) =>
        Prefix + PreviewRoute.Replace("{node}", Uri.EscapeDataString(node)).Replace("{id}", Uri.EscapeDataString(id));

    public static string LibraryList => Prefix + LibraryListRoute;

    public static string Library(string id) => Prefix + LibraryRoute.Replace("{id}", Uri.EscapeDataString(id));

    public static string Cursors(string id) => Prefix + CursorsRoute.Replace("{id}", Uri.EscapeDataString(id));

    public static string Cursor(string id, string alias) =>
        Prefix + CursorRoute.Replace("{id}", Uri.EscapeDataString(id)).Replace("{alias}", Uri.EscapeDataString(alias));

    /// <summary>What an identity key the coordinator makes begins with: <c>dtp_{client id}_{secret}</c>.</summary>
    public const string KeyPrefix = "dtp_";

    /// <summary>The length of a key's secret: 256 random bits as base64url, so the client id reads from the end although base64url holds '_'.</summary>
    public const int KeySecretLength = 43;

    private const int KeySecretBytes = 32;

    /// <summary>
    /// A new key for the identity <paramref name="clientId"/>: <c>dtp_{client id}_{secret}</c>, the secret 256 random bits as
    /// base64url. The one place a key is made, for the server and for <c>dtpipe keygen</c>. A client id that
    /// <see cref="ClientIdOfKey"/> would not read back makes a key that opens nothing: the caller checks it with <see cref="IsPipelineId"/>.
    /// </summary>
    public static string NewKey(string clientId) =>
        $"{KeyPrefix}{clientId}_{Base64Url.EncodeToString(RandomNumberGenerator.GetBytes(KeySecretBytes))}";

    /// <summary>
    /// The client id an identity key names, or null when <paramref name="key"/> does not have a key's shape. The coordinator finds the
    /// identity by it and judges the key by its hash alone; the studio sends it as the <c>client_id</c> its key is exchanged under.
    /// </summary>
    public static string? ClientIdOfKey(string? key)
    {
        if (key is null || !key.StartsWith(KeyPrefix, StringComparison.Ordinal) || key.Length <= KeyPrefix.Length + KeySecretLength + 1)
            return null;
        var separator = key.Length - KeySecretLength - 1;
        if (key[separator] != '_') return null;
        var clientId = key[KeyPrefix.Length..separator];
        return IsPipelineId(clientId) ? clientId : null;
    }

    /// <summary>A pipeline id of the library: lowercase letters, digits and '-', a letter or digit first.</summary>
    public static bool IsPipelineId(string? id) => id is not null && PipelineId().IsMatch(id);

    // \z, not $: '$' also matches before a final newline.
    [GeneratedRegex(@"^[a-z0-9][a-z0-9-]{0,62}\z")]
    private static partial Regex PipelineId();
}
