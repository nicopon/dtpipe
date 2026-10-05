using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using OpenIddict.Abstractions;
using static OpenIddict.Abstractions.OpenIddictConstants;

namespace DtPipe.Lab.Idp;

public sealed class IdpDbContext(DbContextOptions<IdpDbContext> options) : DbContext(options);

[JsonConverter(typeof(JsonStringEnumConverter<ClientGrant>))]
public enum ClientGrant
{
    /// <summary>A service: authenticates as itself with its secret (<c>client_credentials</c>).</summary>
    [JsonStringEnumMemberName("client_credentials")] ClientCredentials,

    /// <summary>A front end: signs a user in (<c>authorization_code</c> with PKCE).</summary>
    [JsonStringEnumMemberName("authorization_code")] AuthorizationCode,
}

/// <summary>
/// One client of the clients file. A client-credentials client needs a secret. An authorization-code
/// client with a secret is confidential (a server-side front end); without one it is public.
/// </summary>
public sealed record ClientDefinition(
    string ClientId, ClientGrant Grant, string? Secret = null, string? DisplayName = null,
    IReadOnlyList<string>? RedirectUris = null, IReadOnlyList<string>? PostLogoutRedirectUris = null);

public sealed record ClientsFile(IReadOnlyList<ClientDefinition> Clients);

/// <summary>
/// Registers the clients file with OpenIddict at start. The file is the only source: the store is in
/// memory, so a change takes effect at the next start. The IdP says who a client or a user is, never
/// what either may do: no client carries a group, a role or a permission beyond the protocol's.
/// </summary>
public sealed class ClientCatalog(IServiceProvider services, IdpOptions options, ILogger<ClientCatalog> logger) : IHostedService
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
        }
        return errors.Count == 0
            ? file.Clients
            : throw new InvalidOperationException($"{path}: " + string.Join(" ", errors));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var clients = Load(options.ClientsFile);
        await using var scope = services.CreateAsyncScope();
        var manager = scope.ServiceProvider.GetRequiredService<IOpenIddictApplicationManager>();
        foreach (var client in clients)
            await manager.CreateAsync(Describe(client), cancellationToken);
        logger.LogInformation("IdP: {Count} clients registered from {File}", clients.Count, options.ClientsFile);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static OpenIddictApplicationDescriptor Describe(ClientDefinition client)
    {
        var descriptor = new OpenIddictApplicationDescriptor
        {
            ClientId = client.ClientId,
            ClientSecret = string.IsNullOrEmpty(client.Secret) ? null : client.Secret,
            ClientType = string.IsNullOrEmpty(client.Secret) ? ClientTypes.Public : ClientTypes.Confidential,
            DisplayName = client.DisplayName ?? client.ClientId,
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
                foreach (var uri in client.RedirectUris ?? []) descriptor.RedirectUris.Add(new Uri(uri));
                foreach (var uri in client.PostLogoutRedirectUris ?? []) descriptor.PostLogoutRedirectUris.Add(new Uri(uri));
                break;
        }
        return descriptor;
    }
}
