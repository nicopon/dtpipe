using System.Security.Claims;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;

namespace DtPipe.Lab.Coordinator;

/// <summary>
/// The coordinator trusts an external OpenID Connect provider to say <b>who</b> a caller is, and
/// decides <b>what</b> it may do itself: a token is accepted when the provider signed it for this
/// coordinator's audience, and the caller's group comes from <see cref="RightsStore"/>, never from the
/// token.
/// </summary>
public static class TrustedIdp
{
    public static void AddTrustedIdp(this IServiceCollection services, LabOptions lab)
    {
        services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme).AddJwtBearer(o =>
        {
            o.Authority = lab.Authority;
            o.Audience = lab.Audience;
            // A test provider on plain HTTP; a real deployment points Authority at an HTTPS issuer.
            o.RequireHttpsMetadata = lab.Authority.StartsWith("https://", StringComparison.OrdinalIgnoreCase);
            o.MapInboundClaims = false;
            o.TokenValidationParameters = new TokenValidationParameters { NameClaimType = "sub" };
            // The node channel is SignalR, which may carry its token as the access_token query.
            o.Events = new JwtBearerEvents
            {
                OnMessageReceived = context =>
                {
                    if (context.Request.Path.StartsWithSegments(DtPipe.Coordinator.Contracts.NodeChannel.Path)
                        && context.Request.Query["access_token"] is { Count: > 0 } token)
                        context.Token = token;
                    return Task.CompletedTask;
                },
            };
        });
        services.AddAuthorization();
    }

    /// <summary>The <c>sub</c> of an authenticated caller: the client id the provider issued its token to.</summary>
    public static string? SubjectOf(ClaimsPrincipal? user) => user?.FindFirst("sub")?.Value;
}

/// <summary>
/// TransportR's identity, with the groups the rights document gives the token's subject instead of
/// the token's own scopes. TransportR reads them at <c>Connect</c>, on every stream and at each token
/// renewal; an unknown or disabled subject resolves with no group, which TransportR refuses at
/// <c>Connect</c> and applies as a withdrawal at renewal. Everything else (client id, expiry,
/// subject) stays TransportR's own reading of the token.
/// </summary>
public sealed class RightsIdentityProvider(RightsStore rights) : IClientIdentityProvider
{
    private readonly JwtIdentityProvider _token = new();

    public ClientIdentity? ResolveIdentity(ClaimsPrincipal? user, Guid? providedClientId, IEnumerable<string>? providedGroups)
    {
        var identity = _token.ResolveIdentity(user, providedClientId, providedGroups);
        if (identity is null) return null;
        var granted = rights.Find(identity.Subject) is { Enabled: true } known ? new HashSet<string> { known.Group } : [];
        return identity with { Groups = granted };
    }
}
