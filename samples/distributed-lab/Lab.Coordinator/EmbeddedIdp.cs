using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using OpenIddict.Abstractions;
using OpenIddict.Server;
using OpenIddict.Server.AspNetCore;
using static OpenIddict.Abstractions.OpenIddictConstants;

namespace DtPipe.Lab.Coordinator;

public sealed class IdpDbContext(DbContextOptions<IdpDbContext> options) : DbContext(options);

/// <summary>
/// The coordinator's own identity provider, after TransportR's SimpleIdp: OpenIddict, the
/// client-credentials flow, and the TransportR group carried as a <c>transportr:group:&lt;g&gt;</c>
/// scope, which TransportR's <c>JwtIdentityProvider</c> reads. Unlike SimpleIdp, its clients are
/// the identities of <see cref="RightsStore"/>, synchronised whenever the rights change, and a
/// token's group is the identity's group at issuance, whatever scope the client asked for.
/// The signing key is generated once and kept in the state directory (<see cref="PersistentSigningKey"/>):
/// a node host reuses its cached token until shortly before it expires, so the tokens must stay valid
/// across a coordinator restart.
/// </summary>
public static class EmbeddedIdp
{
    public const string TokenPath = "/connect/token";
    public const string GroupScopePrefix = "transportr:group:";

    public static void AddEmbeddedIdp(this IServiceCollection services, LabOptions lab)
    {
        services.AddDbContext<IdpDbContext>(o =>
        {
            o.UseInMemoryDatabase("lab-idp");
            o.UseOpenIddict();
        });

        services.AddOpenIddict()
            .AddCore(o => o.UseEntityFrameworkCore().UseDbContext<IdpDbContext>())
            .AddServer(o =>
            {
                o.SetIssuer(new Uri(lab.PublicUrl + "/"));
                o.SetTokenEndpointUris(TokenPath.TrimStart('/'));
                o.AllowClientCredentialsFlow();
                // Access tokens are signed, not encrypted, and the client-credentials flow issues nothing
                // else: only the signing key has to outlive a restart.
                o.AddEphemeralEncryptionKey();
                o.AddSigningKey(PersistentSigningKey.LoadOrCreate(Path.Combine(lab.StateDir, "idp", "signing-key.pem")));
                o.DisableAccessTokenEncryption();
                o.SetAccessTokenLifetime(lab.TokenLifetime);
                o.UseAspNetCore().EnableTokenEndpointPassthrough().DisableTransportSecurityRequirement();
            });

        // Tokens are checked against the signing keys of the server above, in process: no metadata
        // round trip to the coordinator's own URL.
        services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme).AddJwtBearer();
        services.AddOptions<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme)
            .Configure<IOptionsMonitor<OpenIddictServerOptions>>((o, server) =>
            {
                o.MapInboundClaims = false;
                o.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidIssuer = lab.PublicUrl + "/",
                    ValidateAudience = false,
                    ValidateLifetime = true,
                    IssuerSigningKeyResolver = (_, _, _, _) => server.CurrentValue.SigningCredentials.Select(c => c.Key),
                };
                // The lab's control channel may also carry its token as SignalR's access_token query.
                o.Events = new JwtBearerEvents
                {
                    OnMessageReceived = context =>
                    {
                        if (context.Request.Path.StartsWithSegments(Contracts.LabHubMethods.Path) && context.Request.Query["access_token"] is { Count: > 0 } token)
                            context.Token = token;
                        return Task.CompletedTask;
                    },
                };
            });
        services.AddAuthorization();
        services.AddHostedService<IdpClientSync>();
    }

    /// <summary>
    /// The token endpoint, past OpenIddict's own checks (known client, right secret): the token
    /// names the client and carries its identity's one group.
    /// </summary>
    public static void MapEmbeddedIdp(this WebApplication app) => app.MapPost(TokenPath, (HttpContext context, RightsStore rights) =>
    {
        var request = Microsoft.AspNetCore.OpenIddictServerAspNetCoreHelpers.GetOpenIddictServerRequest(context) ?? throw new InvalidOperationException("Not an OpenID Connect request.");
        var identity = request.IsClientCredentialsGrantType() ? rights.Find(request.ClientId) : null;
        if (identity is not { Enabled: true })
        {
            return Results.Forbid(new AuthenticationProperties(new Dictionary<string, string?>
            {
                [OpenIddictServerAspNetCoreConstants.Properties.Error] = Errors.InvalidClient,
                [OpenIddictServerAspNetCoreConstants.Properties.ErrorDescription] = "Unknown or disabled client, or not a client-credentials request.",
            }), [OpenIddictServerAspNetCoreDefaults.AuthenticationScheme]);
        }

        var claims = new ClaimsIdentity(OpenIddictServerAspNetCoreDefaults.AuthenticationScheme, Claims.Name, Claims.Role);
        claims.SetClaim(Claims.Subject, identity.ClientId);
        claims.SetClaim(Claims.Name, identity.DisplayName);
        claims.SetScopes(GroupScopePrefix + identity.Group);
        claims.SetDestinations(_ => [Destinations.AccessToken]);
        return Results.SignIn(new ClaimsPrincipal(claims), authenticationScheme: OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
    });

    /// <summary>The <c>sub</c> of an authenticated caller: the identity's client id.</summary>
    public static string? ClientIdOf(ClaimsPrincipal? user) => user?.FindFirst(Claims.Subject)?.Value;
}

/// <summary>
/// Keeps OpenIddict's registered clients equal to the enabled identities: a disabled or removed
/// identity can no longer obtain a token, and a new one can at once.
/// </summary>
public sealed class IdpClientSync(IServiceProvider services, RightsStore rights, ILogger<IdpClientSync> logger) : IHostedService
{
    private readonly SemaphoreSlim _gate = new(1, 1);

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        rights.Changed += () => _ = Task.Run(() => SyncAsync(CancellationToken.None));
        await SyncAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task SyncAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            await using var scope = services.CreateAsyncScope();
            var manager = scope.ServiceProvider.GetRequiredService<IOpenIddictApplicationManager>();
            var wanted = rights.Snapshot().Identities.Where(i => i.Enabled).ToDictionary(i => i.ClientId);

            var existing = new List<object>();
            await foreach (var application in manager.ListAsync(cancellationToken: ct)) existing.Add(application);
            foreach (var application in existing)
            {
                var id = await manager.GetClientIdAsync(application, ct);
                // Recreated each time: the secret or display name may have changed, and deleting is cheap.
                await manager.DeleteAsync(application, ct);
                if (id is not null && !wanted.ContainsKey(id)) logger.LogInformation("IDP: client {ClientId} can no longer obtain tokens", id);
            }
            foreach (var identity in wanted.Values)
            {
                var descriptor = new OpenIddictApplicationDescriptor
                {
                    ClientId = identity.ClientId,
                    ClientSecret = identity.Secret,
                    DisplayName = identity.DisplayName,
                    ClientType = ClientTypes.Confidential,
                };
                descriptor.Permissions.Add(Permissions.Endpoints.Token);
                descriptor.Permissions.Add(Permissions.GrantTypes.ClientCredentials);
                await manager.CreateAsync(descriptor, ct);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "IDP: client synchronisation failed");
        }
        finally
        {
            _gate.Release();
        }
    }
}
