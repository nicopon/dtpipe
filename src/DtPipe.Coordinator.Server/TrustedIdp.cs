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
/// decides <b>what</b> it may do itself. Two kinds of caller, told apart by the token's claims, never by
/// what a caller says of itself:
/// <list type="bullet">
/// <item>a service (a node, the runner) presents a bearer token the provider signed for this
///   coordinator's audience through client credentials, so its subject is its client
///   (<c>sub == client_id</c>); its group comes from <see cref="RightsStore"/>, never from the token.
///   The default policy, which the node channel and TransportR's hub and streams use, accepts only
///   this scheme (<see cref="ServiceScheme"/>);</item>
/// <item>a person signs in through the provider (authorization code with PKCE) and holds a session
///   cookie, or presents a bearer token <c>dtpipe studio</c> obtained for them through
///   <c>Coordinator:Studio:ClientId</c> (<see cref="PersonScheme"/>: that client, and a subject that is
///   not it). Their roles come from the server's configuration (<see cref="CoordinatorRoles"/>), never
///   from the token. <c>/api</c> accepts either (<see cref="ApiScheme"/>); the pages only the cookie.</item>
/// </list>
/// Each scheme refuses the other kind of token when it validates it, so a person's token never
/// reaches the data plane, whichever endpoint reads it, and a service's never reaches <c>/api</c>.
/// </summary>
public static class TrustedIdp
{
    public const string CookieScheme = CookieAuthenticationDefaults.AuthenticationScheme;
    public const string OidcScheme = OpenIdConnectDefaults.AuthenticationScheme;
    public const string ServiceScheme = JwtBearerDefaults.AuthenticationScheme;
    public const string PersonScheme = "PersonBearer";

    /// <summary>What <c>/api</c> authenticates with: a person's bearer token when the request carries one, else the cookie.</summary>
    public const string ApiScheme = "Api";

    /// <summary>
    /// The header a request to <c>/api</c> that changes something must carry, a bearer's too. A page of
    /// another site cannot set it without a CORS preflight, which this server never answers, so it
    /// is what stops a forged request riding the session cookie.
    /// </summary>
    public const string CsrfHeader = DtPipe.Jobs.Coordinator.CoordinatorApi.CsrfHeader;

    public static void AddTrustedIdp(this IServiceCollection services, CoordinatorServerOptions server)
    {
        var https = server.Authority.StartsWith("https://", StringComparison.OrdinalIgnoreCase);
        void Trust(JwtBearerOptions o)
        {
            o.Authority = server.Authority;
            o.Audience = server.Audience;
            // A test provider on plain HTTP; a real deployment points Authority at an HTTPS issuer.
            o.RequireHttpsMetadata = https;
            o.MapInboundClaims = false;
            o.TokenValidationParameters = new TokenValidationParameters { NameClaimType = "sub" };
        }

        services.AddAuthentication(o =>
            {
                o.DefaultScheme = CookieScheme;
                o.DefaultChallengeScheme = OidcScheme;
            })
            .AddJwtBearer(ServiceScheme, o =>
            {
                Trust(o);
                o.Events = new JwtBearerEvents
                {
                    // The node channel is SignalR, which may carry its token as the access_token query.
                    OnMessageReceived = context =>
                    {
                        if (context.Request.Path.StartsWithSegments(DtPipe.Coordinator.Contracts.NodeChannel.Path)
                            && context.Request.Query["access_token"] is { Count: > 0 } token)
                            context.Token = token;
                        return Task.CompletedTask;
                    },
                    OnTokenValidated = context =>
                    {
                        if (!IsService(context.Principal)) context.Fail("Only a service's token reaches the data plane.");
                        return Task.CompletedTask;
                    },
                };
            })
            .AddJwtBearer(PersonScheme, o =>
            {
                Trust(o);
                o.TokenValidationParameters.NameClaimType = "name";
                o.Events = new JwtBearerEvents
                {
                    OnTokenValidated = context =>
                    {
                        if (!IsStudioPerson(context.Principal, server.StudioClientId))
                            context.Fail("Only a person's token obtained through the studio's client reaches /api.");
                        return Task.CompletedTask;
                    },
                };
            })
            .AddPolicyScheme(ApiScheme, ApiScheme, o => o.ForwardDefaultSelector = context =>
                context.Request.Headers.Authorization.ToString().StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? PersonScheme : CookieScheme)
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
            o.DefaultPolicy = new AuthorizationPolicyBuilder(ServiceScheme).RequireAuthenticatedUser().Build();
            foreach (var role in CoordinatorRoles.All)
                o.AddPolicy(role, p => p.AddAuthenticationSchemes(ApiScheme).RequireRole(role));
            o.AddPolicy(SignedIn, p => p.AddAuthenticationSchemes(ApiScheme).RequireAuthenticatedUser());
            o.AddPolicy(Monitor, p => p.AddAuthenticationSchemes(ServiceScheme).RequireAuthenticatedUser().AddRequirements(new MonitorRequirement()));
        });
        services.AddSingleton<IAuthorizationHandler, MonitorHandler>();
        services.AddSingleton<IClaimsTransformation, CoordinatorRoles>();
        // The session cookie is encrypted with keys kept in the state directory, so a restart does
        // not sign every person out.
        services.AddDataProtection().PersistKeysToFileSystem(new DirectoryInfo(Path.Combine(server.StateDir, "keys")));
    }

    /// <summary>A person signed in, whatever their role: who they are is all <c>/api/me</c> needs.</summary>
    public const string SignedIn = "signed-in";

    /// <summary>
    /// A service the rights grant <see cref="IdentityKinds.Monitor"/> to: the only caller of <c>/health</c>. It is a service's token
    /// (<see cref="ServiceScheme"/>), and what it may do is the rights', so a node, an identity of another kind, an unknown or a
    /// disabled one is refused with its valid token.
    /// </summary>
    public const string Monitor = "monitor";

    /// <summary>The email claim of the signed-in person, or null when there is none.</summary>
    public static string? EmailOf(HttpContext context) => context.User.FindFirst("email")?.Value;

    /// <summary>A page a browser opens: the only requests a missing session sends to the provider.</summary>
    public static bool IsPageNavigation(HttpRequest request) =>
        HttpMethods.IsGet(request.Method)
        && (request.Path == "/" || request.Path == "/index.html" || request.Path.StartsWithSegments("/lab")
            || request.Path.StartsWithSegments("/account"));

    /// <summary>
    /// A client-credentials token: no resource owner, so its subject is its client (RFC 9068). A
    /// provider whose service tokens name another subject has its services refused here: fail-closed.
    /// </summary>
    public static bool IsService(ClaimsPrincipal? token) =>
        token?.FindFirst("sub")?.Value is { Length: > 0 } subject && subject == token.FindFirst("client_id")?.Value;

    /// <summary>A person's token obtained through <paramref name="studioClientId"/>: that client, and a subject that is not it.</summary>
    public static bool IsStudioPerson(ClaimsPrincipal? token, string? studioClientId) =>
        !string.IsNullOrEmpty(studioClientId)
        && token?.FindFirst("client_id")?.Value == studioClientId
        && token.FindFirst("sub")?.Value is { Length: > 0 } subject && subject != studioClientId;

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
        // naming a person, so it gains none; the policies that grant roles accept a person only.
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

/// <summary>The caller's identity in the rights is an enabled <see cref="IdentityKinds.Monitor"/>.</summary>
public sealed class MonitorRequirement : IAuthorizationRequirement;

/// <summary>Grants <see cref="MonitorRequirement"/> by what the rights say of the token's subject, never by anything the token carries.</summary>
public sealed class MonitorHandler(RightsStore rights) : AuthorizationHandler<MonitorRequirement>
{
    protected override Task HandleRequirementAsync(AuthorizationHandlerContext context, MonitorRequirement requirement)
    {
        if (rights.Find(TrustedIdp.SubjectOf(context.User)) is { Enabled: true, Kind: IdentityKinds.Monitor }) context.Succeed(requirement);
        return Task.CompletedTask;
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
        // Only a node belongs to groups: any other kind of identity is never admitted to the data plane.
        var granted = rights.Find(identity.Subject) is { Enabled: true, IsNode: true } known ? known.Groups.ToHashSet() : [];
        return identity with { Groups = granted };
    }
}
