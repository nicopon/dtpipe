using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using OpenIddict.Abstractions;
using static OpenIddict.Abstractions.OpenIddictConstants;

namespace DtPipe.Coordinator.TrialIdp;

public sealed class IdpDbContext(DbContextOptions<IdpDbContext> options) : DbContext(options);

[JsonConverter(typeof(JsonStringEnumConverter<ClientGrant>))]
public enum ClientGrant
{
    /// <summary>A service: authenticates as itself with its secret (<c>client_credentials</c>).</summary>
    [JsonStringEnumMemberName("client_credentials")] ClientCredentials,

    /// <summary>A front end: signs a user in (<c>authorization_code</c> with PKCE).</summary>
    [JsonStringEnumMemberName("authorization_code")] AuthorizationCode,
}

[JsonConverter(typeof(JsonStringEnumConverter<ClientApplication>))]
public enum ClientApplication
{
    /// <summary>A front end served from a server, at the redirect URIs it registers.</summary>
    [JsonStringEnumMemberName("web")] Web,

    /// <summary>
    /// An application on a person's machine that listens on the loopback (RFC 8252): a redirect URI
    /// registered without a port accepts any port, the rest unchanged.
    /// </summary>
    [JsonStringEnumMemberName("native")] Native,
}

/// <summary>
/// One client of the clients file. A client-credentials client needs a secret. An authorization-code
/// client with a secret is confidential (a server-side front end); without one it is public.
/// <see cref="Audience"/> is the service a person's access token obtained through this client is meant
/// for (none by default), and <see cref="RefreshTokens"/> lets the client keep a person signed in
/// (<c>offline_access</c>): both are protocol, not a right.
/// </summary>
public sealed record ClientDefinition(
    string ClientId, ClientGrant Grant, string? Secret = null, string? DisplayName = null,
    IReadOnlyList<string>? RedirectUris = null, IReadOnlyList<string>? PostLogoutRedirectUris = null,
    ClientApplication ApplicationType = ClientApplication.Web, string? Audience = null, bool RefreshTokens = false);

public sealed record ClientsFile(IReadOnlyList<ClientDefinition> Clients);

/// <summary>
/// Registers the clients file with OpenIddict at start. The file is the only source: the store is in
/// memory, so a change takes effect at the next start. The IdP says who a client or a user is, never
/// what either may do: no client carries a group, a role or a permission beyond the protocol's.
/// </summary>
public sealed class ClientCatalog(IServiceProvider services, IdpOptions options, IReadOnlyList<ClientDefinition> clients, ILogger<ClientCatalog> logger) : IHostedService
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static IReadOnlyList<ClientDefinition> Load(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"The clients file {path} does not exist (Idp:ClientsFile).", path);
        var file = JsonSerializer.Deserialize<ClientsFile>(File.ReadAllText(path), Json)
            ?? throw new InvalidOperationException($"{path}: empty clients file.");

        var errors = new List<string>();
        foreach (var duplicate in file.Clients.GroupBy(c => c.ClientId, StringComparer.Ordinal).Where(g => g.Count() > 1))
            errors.Add($"'{duplicate.Key}' is declared {duplicate.Count()} times.");
        foreach (var client in file.Clients)
        {
            if (string.IsNullOrWhiteSpace(client.ClientId)) errors.Add("a client has no clientId.");
            if (client.Grant == ClientGrant.ClientCredentials && string.IsNullOrEmpty(client.Secret))
                errors.Add($"'{client.ClientId}' uses client_credentials and has no secret.");
            if (client.Grant == ClientGrant.AuthorizationCode && client.RedirectUris is not { Count: > 0 })
                errors.Add($"'{client.ClientId}' uses authorization_code and has no redirectUris.");
            if (client.Grant != ClientGrant.AuthorizationCode && (client.Audience is not null || client.RefreshTokens || client.ApplicationType != ClientApplication.Web))
                errors.Add($"'{client.ClientId}': audience, refreshTokens and applicationType belong to an authorization_code client.");
            if (client.ApplicationType == ClientApplication.Native && !string.IsNullOrEmpty(client.Secret))
                errors.Add($"'{client.ClientId}' is native: an application on a person's machine keeps no secret.");
        }
        return errors.Count == 0
            ? file.Clients
            : throw new InvalidOperationException($"{path}: " + string.Join(" ", errors));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var scope = services.CreateAsyncScope();
        var manager = scope.ServiceProvider.GetRequiredService<IOpenIddictApplicationManager>();
        foreach (var client in clients)
            await manager.CreateAsync(Describe(client, options.ExtraOrigins), cancellationToken);
        logger.LogInformation("IdP: {Count} clients registered from {File}", clients.Count, options.ClientsFile);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static OpenIddictApplicationDescriptor Describe(ClientDefinition client, IReadOnlyList<Uri> extraOrigins)
    {
        // A registered URI, and the same path at every extra origin. Uri normalizes a default port
        // away, as the redirect URI a front end on that port sends.
        static IEnumerable<Uri> AtEveryOrigin(IEnumerable<string>? uris, IReadOnlyList<Uri> origins) => (uris ?? []).Select(u => new Uri(u))
            .SelectMany(u => origins.Select(o => new Uri(o, u.PathAndQuery)).Prepend(u));

        var descriptor = new OpenIddictApplicationDescriptor
        {
            ClientId = client.ClientId,
            ClientSecret = string.IsNullOrEmpty(client.Secret) ? null : client.Secret,
            ClientType = string.IsNullOrEmpty(client.Secret) ? ClientTypes.Public : ClientTypes.Confidential,
            DisplayName = client.DisplayName ?? client.ClientId,
            ApplicationType = client.ApplicationType == ClientApplication.Native ? ApplicationTypes.Native : ApplicationTypes.Web,
        };
        descriptor.Permissions.Add(Permissions.Endpoints.Token);

        switch (client.Grant)
        {
            case ClientGrant.ClientCredentials:
                descriptor.Permissions.Add(Permissions.GrantTypes.ClientCredentials);
                break;
            case ClientGrant.AuthorizationCode:
                // No consent screen: a test IdP signs a user straight into the front end that asked.
                descriptor.ConsentType = ConsentTypes.Implicit;
                descriptor.Permissions.UnionWith(
                [
                    Permissions.Endpoints.Authorization,
                    Permissions.Endpoints.EndSession,
                    Permissions.GrantTypes.AuthorizationCode,
                    Permissions.ResponseTypes.Code,
                    Permissions.Scopes.Email,
                    Permissions.Scopes.Profile,
                ]);
                descriptor.Requirements.Add(Requirements.Features.ProofKeyForCodeExchange);
                if (client.RefreshTokens) descriptor.Permissions.Add(Permissions.GrantTypes.RefreshToken);
                // A native client listens on the loopback of a person's machine, never at an extra origin.
                var origins = client.ApplicationType == ClientApplication.Native ? [] : extraOrigins;
                foreach (var uri in AtEveryOrigin(client.RedirectUris, origins)) descriptor.RedirectUris.Add(uri);
                foreach (var uri in AtEveryOrigin(client.PostLogoutRedirectUris, origins)) descriptor.PostLogoutRedirectUris.Add(uri);
                break;
        }
        return descriptor;
    }
}
