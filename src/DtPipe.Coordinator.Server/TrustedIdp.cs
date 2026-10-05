using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator trusts an external OpenID Connect provider to say <b>who</b> a caller is, and
/// decides <b>what</b> it may do itself. Two kinds of caller, two schemes:
/// <list type="bullet">
/// <item>a service (a node, the runner) presents a bearer token the provider signed for this
///   coordinator's audience; its group comes from <see cref="RightsStore"/>, never from the token.
///   The default policy, which the node channel and TransportR's hub and streams use, accepts
///   only this scheme;</item>
/// <item>a person signs in through the provider (authorization code with PKCE) and holds a session
///   cookie; their roles come from the server's configuration (<see cref="CoordinatorRoles"/>), never
///   from the token. <c>/api</c> and the pages accept only this scheme.</item>
/// </list>
/// </summary>
public static class TrustedIdp
{
    public const string CookieScheme = CookieAuthenticationDefaults.AuthenticationScheme;
    public const string OidcScheme = OpenIdConnectDefaults.AuthenticationScheme;

    /// <summary>
    /// The header a cookie-authenticated request that changes something must carry. A page of
    /// another site cannot set it without a CORS preflight, which this server never answers, so it
    /// is what stops a forged request riding the session cookie.
    /// </summary>
    public const string CsrfHeader = "X-Dtpipe-Csrf";

    public static void AddTrustedIdp(this IServiceCollection services, CoordinatorServerOptions server)
    {
        var https = server.Authority.StartsWith("https://", StringComparison.OrdinalIgnoreCase);
        services.AddAuthentication(o =>
            {
                o.DefaultScheme = CookieScheme;
                o.DefaultChallengeScheme = OidcScheme;
            })
            .AddJwtBearer(o =>
            {
                o.Authority = server.Authority;
                o.Audience = server.Audience;
                // A test provider on plain HTTP; a real deployment points Authority at an HTTPS issuer.
                o.RequireHttpsMetadata = https;
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
            })
            .AddCookie(CookieScheme, o =>
            {
                o.Cookie.Name = "dtpipe-coordinator";
                o.Cookie.HttpOnly = true;
                o.Cookie.SameSite = SameSiteMode.Lax;
                o.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
                o.ExpireTimeSpan = TimeSpan.FromHours(8);
                o.SlidingExpiration = true;
                // An API caller is told, never redirected: only a page navigation goes to the provider.
                o.Events.OnRedirectToAccessDenied = context =>
                {
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return Task.CompletedTask;
                };
            })
            .AddOpenIdConnect(OidcScheme, o =>
            {
                o.Authority = server.Authority;
                o.RequireHttpsMetadata = https;
                o.ClientId = server.UiClientId;
                o.ClientSecret = server.UiClientSecret;
                o.ResponseType = OpenIdConnectResponseType.Code;
                o.UsePkce = true;
                // The code comes back in the query: a top-level GET carries the Lax correlation cookies,
                // where a cross-site form post would need SameSite=None, which plain HTTP cannot keep.
                o.ResponseMode = OpenIdConnectResponseMode.Query;
                o.Scope.Clear();
                foreach (var scope in new[] { "openid", "email", "profile" }) o.Scope.Add(scope);
                o.MapInboundClaims = false;
                o.TokenValidationParameters.NameClaimType = "name";
                // The identity token is kept to name the session when the user signs out.
                o.SaveTokens = true;
                o.GetClaimsFromUserInfoEndpoint = false;
                o.CorrelationCookie.SameSite = SameSiteMode.Lax;
                o.CorrelationCookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
                o.NonceCookie.SameSite = SameSiteMode.Lax;
                o.NonceCookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
                o.Events = new OpenIdConnectEvents
                {
                    OnRedirectToIdentityProvider = context =>
                    {
                        if (IsPageNavigation(context.Request)) return Task.CompletedTask;
                        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                        context.HandleResponse();
                        return Task.CompletedTask;
                    },
                };
            });

        services.AddAuthorization(o =>
        {
            // The node channel and TransportR's endpoints map RequireAuthorization() with no policy:
            // they accept a service's bearer token and nothing else.
            o.DefaultPolicy = new AuthorizationPolicyBuilder(JwtBearerDefaults.AuthenticationScheme).RequireAuthenticatedUser().Build();
            foreach (var role in CoordinatorRoles.All)
                o.AddPolicy(role, p => p.AddAuthenticationSchemes(CookieScheme).RequireRole(role));
            o.AddPolicy(SignedIn, p => p.AddAuthenticationSchemes(CookieScheme).RequireAuthenticatedUser());
        });
        services.AddSingleton<IClaimsTransformation, CoordinatorRoles>();
        // The session cookie is encrypted with keys kept in the state directory, so a restart does
        // not sign every person out.
        services.AddDataProtection().PersistKeysToFileSystem(new DirectoryInfo(Path.Combine(server.StateDir, "keys")));
    }

    /// <summary>A person signed in, whatever their role: who they are is all <c>/api/me</c> needs.</summary>
    public const string SignedIn = "signed-in";

    /// <summary>A page a browser opens: the only requests a missing session sends to the provider.</summary>
    public static bool IsPageNavigation(HttpRequest request) =>
        HttpMethods.IsGet(request.Method)
        && (request.Path == "/" || request.Path == "/index.html" || request.Path.StartsWithSegments("/lab")
            || request.Path.StartsWithSegments("/account"));

    /// <summary>The <c>sub</c> of an authenticated caller: the client id the provider issued its token to.</summary>
    public static string? SubjectOf(ClaimsPrincipal? user) => user?.FindFirst("sub")?.Value;
}

/// <summary>
/// A person's roles on this coordinator, from its configuration: <c>Coordinator:Roles:Admin</c>,
/// <c>:Designer</c> and <c>:Reader</c> each list the people (by the claim <c>Coordinator:Roles:Claim</c>,
/// <c>email</c> by default) who hold it, separated by commas or semicolons, compared without case.
/// Roles are cumulative (an admin is a designer, a designer a reader), and a person in no list holds
/// none: the provider says who they are, never what they may do. Applied to every request, so a list
/// changed in configuration takes effect without signing anyone out.
/// </summary>
public sealed class CoordinatorRoles(IConfiguration configuration) : IClaimsTransformation
{
    public const string Admin = "admin";
    public const string Designer = "designer";
    public const string Reader = "reader";
    public static readonly IReadOnlyList<string> All = [Admin, Designer, Reader];

    private const string Issuer = "dtpipe-coordinator";

    public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal)
    {
        // A principal already transformed carries our roles. A service's token carries no claim
        // naming a person, so it gains none; the policies that grant roles accept the cookie only.
        if (principal.Identity is not ClaimsIdentity { IsAuthenticated: true } identity
            || identity.HasClaim(c => c.Type == identity.RoleClaimType && c.Issuer == Issuer))
            return Task.FromResult(principal);

        var section = configuration.GetSection("Coordinator:Roles");
        var who = principal.FindFirst(section["Claim"] ?? "email")?.Value;
        if (string.IsNullOrWhiteSpace(who)) return Task.FromResult(principal);

        bool Listed(string key) => (section[key] ?? "")
            .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Contains(who.Trim(), StringComparer.OrdinalIgnoreCase);

        var granted = Listed("Admin") ? All
            : Listed("Designer") ? [Designer, Reader]
            : Listed("Reader") ? [Reader]
            : [];
        var transformed = new ClaimsIdentity(identity);
        foreach (var role in granted)
            transformed.AddClaim(new Claim(transformed.RoleClaimType, role, ClaimValueTypes.String, Issuer));
        return Task.FromResult(new ClaimsPrincipal(transformed));
    }
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
