using System.Security.Cryptography;
using System.Text;
using DtPipe.Coordinator.TrialIdp;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The trial identity provider run inside the coordinator (<see cref="CoordinatorServerOptions.TrialIdpPassword"/>), never for production.
/// Its clients are the identities of the rights, each with the trial password for secret, plus the pages' client and the studio's; its
/// people are any valid name with that same password. The server trusts it exactly as it trusts an external provider: through
/// <see cref="TrustedIdp"/>, by the token's issuer and keys, with no exception for its own tokens.
/// </summary>
public static class TrialIdpMode
{
    /// <summary>The login form's route and session cookie, apart from the server's own (<c>/account/login</c>, <see cref="TrustedIdp.CookieScheme"/>).</summary>
    public const string LoginPath = "/trial-idp/login";

    public static IServiceCollection AddTrialIdpMode(this IServiceCollection services, CoordinatorServerOptions server)
    {
        var password = Encoding.UTF8.GetBytes(server.TrialIdpPassword!);
        services.AddTrialIdp(new TrialIdpSettings
        {
            Issuer = new Uri(server.Authority.TrimEnd('/') + "/"),
            Audience = server.Audience,
            SigningKeyFile = Path.Combine(server.StateDir, "trial-idp-signing-key.pem"),
            EmailDomain = "dtpipe.test",
            CookieScheme = "TrialIdp",
            CookieName = "dtpipe-trial-idp",
            LoginPath = LoginPath,
            CheckUser = (user, given) => TestUsers.IsValidName(user) && given is not null
                && CryptographicOperations.FixedTimeEquals(SHA256.HashData(Encoding.UTF8.GetBytes(given)), SHA256.HashData(password)),
            LoginHint = "Trial identity provider, not for production. Any user name signs in with the trial password the coordinator was started with.",
        });
        services.AddSingleton<TrialIdpClients>();
        return services;
    }
}

/// <summary>
/// Keeps the trial provider's clients equal to the rights: a service the rights name can ask for a token, one they no longer name cannot.
/// A change to the rights only marks the clients stale; they are made right before the next token is issued.
/// </summary>
public sealed class TrialIdpClients
{
    private readonly ClientRegistrar _registrar;
    private readonly RightsStore _rights;
    private readonly CoordinatorServerOptions _server;
    private volatile bool _stale = true;

    public TrialIdpClients(ClientRegistrar registrar, RightsStore rights, CoordinatorServerOptions server)
    {
        (_registrar, _rights, _server) = (registrar, rights, server);
        rights.Changed += () => _stale = true;
    }

    public async Task EnsureCurrentAsync(CancellationToken cancellationToken)
    {
        if (!_stale) return;
        _stale = false;
        try
        {
            await _registrar.SyncAsync(Wanted(), cancellationToken);
        }
        catch
        {
            _stale = true;
            throw;
        }
    }

    private List<ClientDefinition> Wanted()
    {
        var origin = new Uri(_server.Authority.TrimEnd('/') + "/");
        var clients = new List<ClientDefinition>
        {
            new(_server.UiClientId, ClientGrant.AuthorizationCode, _server.UiClientSecret, "Coordinator pages",
                RedirectUris: [new Uri(origin, "signin-oidc").ToString()], PostLogoutRedirectUris: [new Uri(origin, "signout-callback-oidc").ToString()]),
        };
        if (_server.StudioClientId is { Length: > 0 } studio)
            clients.Add(new(studio, ClientGrant.AuthorizationCode, DisplayName: "dtpipe studio", RedirectUris: ["http://127.0.0.1/callback"],
                ApplicationType: ClientApplication.Native, Audience: _server.Audience, RefreshTokens: true));
        foreach (var identity in _rights.Snapshot().Identities.Where(i => clients.All(c => c.ClientId != i.ClientId)))
            clients.Add(new(identity.ClientId, ClientGrant.ClientCredentials, _server.TrialIdpPassword, identity.Name));
        return clients;
    }
}
