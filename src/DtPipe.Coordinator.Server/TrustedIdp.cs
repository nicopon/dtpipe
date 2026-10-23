using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.DataProtection.Repositories;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator trusts an issuer to say <b>who</b> a caller is, and decides <b>what</b> it may do
/// itself: the external OpenID Connect provider, and for the identities verified by a key its own
/// <see cref="KeyIssuer"/>. Two kinds of caller, told apart by the claims of a token the issuer made from
/// the identity's kind, never by what a caller says of itself:
/// <list type="bullet">
/// <item>a service (a node, the runner) presents a bearer token an issuer signed for this
///   coordinator's audience through client credentials, so its subject is its client
///   (<c>sub == client_id</c>); its group comes from <see cref="RightsStore"/>, never from the token.
///   The default policy, which the node channel and TransportR's hub and streams use, accepts only
///   this scheme (<see cref="ServiceScheme"/>), which reads the token's issuer without believing it and
///   hands the token to the one scheme that validates that issuer in full (<see cref="IdpServiceScheme"/>,
///   <see cref="KeyServiceScheme"/>). The issuer must be the one that verifies the identity: an identity
///   verified by a key is opened by the key issuer's tokens only, any other by the provider's only;</item>
/// <item>a person signs in through the provider (authorization code with PKCE), or with a key the coordinator made for them
///   (the sign-in form), and holds a session cookie, or presents a bearer token <c>dtpipe studio</c> obtained for them through
///   <c>Coordinator:Studio:ClientId</c> (<see cref="PersonScheme"/>: that client, and a subject that is not it), from the provider
///   or from the key issuer, read by issuer as a service's is (<see cref="IdpPersonScheme"/>, <see cref="KeyPersonScheme"/>).
///   Their roles come from the server (<see cref="CoordinatorRoles"/>): the configuration's and the rights' for a person of the
///   provider, the rights' for a person verified by a key, never from the token. A person verified by a key is judged against the rights on
///   every request, its cookie as its token, so a revoked key or a disabled identity ends the session at the next one.
///   <c>/api</c> accepts either (<see cref="ApiScheme"/>); the pages only the cookie.</item>
/// </list>
/// Each scheme refuses the other kind of token when it validates it, so a person's token never
/// reaches the data plane, whichever endpoint reads it, and a service's never reaches <c>/api</c>.
/// </summary>
public static class TrustedIdp
{
    public const string CookieScheme = CookieAuthenticationDefaults.AuthenticationScheme;
    public const string OidcScheme = OpenIdConnectDefaults.AuthenticationScheme;
    public const string ServiceScheme = JwtBearerDefaults.AuthenticationScheme;
    public const string IdpServiceScheme = "IdpBearer";
    public const string KeyServiceScheme = "KeyBearer";

    /// <summary>A person's bearer token: reads its issuer without believing it and hands it to <see cref="IdpPersonScheme"/> or <see cref="KeyPersonScheme"/>.</summary>
    public const string PersonScheme = "PersonBearer";
    public const string IdpPersonScheme = "IdpPerson";
    public const string KeyPersonScheme = "KeyPerson";

    /// <summary>
    /// The claim that names the key a person verified by a key signed in with (<see cref="IdentityKey.Id"/>): in the key issuer's token
    /// and in the session cookie, issued by the key issuer, so a provider's token that carries it opens nothing.
    /// </summary>
    public const string KeyIdClaim = "dtpipe_key";

    /// <summary>What <c>/api</c> authenticates with: a person's bearer token when the request carries one, else the cookie.</summary>
    public const string ApiScheme = "Api";

    /// <summary>
    /// The header a request to <c>/api</c> that changes something must carry, a bearer's too. A page of
    /// another site cannot set it without a CORS preflight, which this server never answers, so it
    /// is what stops a forged request riding the session cookie.
    /// </summary>
    public const string CsrfHeader = DtPipe.Jobs.Coordinator.CoordinatorApi.CsrfHeader;

    /// <summary>
    /// Registers the schemes of the issuers the server has: the provider's (<see cref="IdpServiceScheme"/>, <see cref="IdpPersonScheme"/>,
    /// <see cref="OidcScheme"/>) only when <see cref="CoordinatorServerOptions.HasProvider"/>, the key issuer's when there is one. No
    /// selector names a scheme that is not registered: without a provider every token goes to the key issuer's schemes.
    /// </summary>
    public static void AddTrustedIdp(this IServiceCollection services, CoordinatorServerOptions server)
    {
        var https = server.Authority?.StartsWith("https://", StringComparison.OrdinalIgnoreCase) == true;
        void Trust(JwtBearerOptions o)
        {
            o.Authority = server.Authority;
            o.Audience = server.Audience;
            // A test provider on plain HTTP; a real deployment points Authority at an HTTPS issuer.
            o.RequireHttpsMetadata = https;
            o.MapInboundClaims = false;
            o.TokenValidationParameters = new TokenValidationParameters { NameClaimType = "sub" };
        }
        // A token goes to the key issuer's scheme when it names that issuer, or when there is no provider for it to go to instead.
        bool ByKey(string? token) => server.KeyIssuer is { } issuer && (!server.HasProvider || IssuerOf(token) == issuer);

        var authentication = services.AddAuthentication(o =>
            {
                o.DefaultScheme = CookieScheme;
                o.DefaultChallengeScheme = server.HasProvider ? OidcScheme : CookieScheme;
            })
            .AddPolicyScheme(ServiceScheme, ServiceScheme, o => o.ForwardDefaultSelector = context =>
                ByKey(ServiceTokenOf(context.Request)) ? KeyServiceScheme : IdpServiceScheme);
        if (server.HasProvider)
            authentication
                .AddJwtBearer(IdpServiceScheme, o =>
                {
                    Trust(o);
                    o.Events = ServiceEvents(byKey: false);
                })
                .AddJwtBearer(IdpPersonScheme, o =>
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
                });
        if (server.KeyIssuer is { } keyIssuer)
        {
            authentication
                .AddJwtBearer(KeyServiceScheme, o =>
                {
                    o.MapInboundClaims = false;
                    o.Events = ServiceEvents(byKey: true);
                })
                .AddJwtBearer(KeyPersonScheme, o =>
                {
                    o.MapInboundClaims = false;
                    o.Events = KeyPersonEvents(server);
                });
            // The key issuer's tokens are validated with the keys of the store, never through a request to this server's own address.
            foreach (var scheme in new[] { KeyServiceScheme, KeyPersonScheme })
                services.AddOptions<JwtBearerOptions>(scheme).Configure<IssuerKeys>((o, keys) => o.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidIssuer = keyIssuer,
                    ValidAudience = server.Audience,
                    ValidAlgorithms = [SecurityAlgorithms.RsaSha256],
                    IssuerSigningKeyResolver = (_, _, kid, _) => keys.Resolve(kid),
                    // The issuer's clock is this server's, or another instance's over the same store: a revoked key opens nothing for long.
                    ClockSkew = TimeSpan.FromSeconds(30),
                    NameClaimType = "sub",
                });
        }
        authentication
            .AddPolicyScheme(PersonScheme, PersonScheme, o => o.ForwardDefaultSelector = context =>
                ByKey(BearerOf(context.Request)) ? KeyPersonScheme : IdpPersonScheme)
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
                // Without a provider a page's challenge is the cookie's: it goes to the sign-in form.
                o.LoginPath = AccountPages.LoginPath;
                o.ReturnUrlParameter = "returnUrl";
                // An API caller is told, never redirected: only a page navigation goes to the provider.
                o.Events.OnRedirectToAccessDenied = context =>
                {
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return Task.CompletedTask;
                };
                // The session of a person verified by a key holds no key: on every request its identity must still be an enabled person
                // and the key it signed in with still live, or the session ends there.
                o.Events.OnValidatePrincipal = async context =>
                {
                    if (KeyPersonOf(context.Principal, server) is { } person && !StillSignedIn(context.HttpContext, server, person))
                    {
                        context.RejectPrincipal();
                        await context.HttpContext.SignOutAsync(CookieScheme);
                    }
                };
            });
        if (server.HasProvider)
            authentication.AddOpenIdConnect(OidcScheme, o =>
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
            o.AddPolicy(Monitor, p => p.AddAuthenticationSchemes(ServiceScheme).RequireAuthenticatedUser().AddRequirements(new ServiceKindRequirement(IdentityKinds.Monitor)));
            o.AddPolicy(Launcher, p => p.AddAuthenticationSchemes(ServiceScheme).RequireAuthenticatedUser().AddRequirements(new ServiceKindRequirement(IdentityKinds.Launcher)));
        });
        services.AddSingleton<IAuthorizationHandler, ServiceKindHandler>();
        services.AddSingleton<IClaimsTransformation, CoordinatorRoles>();
        services.AddSessionKeys();
    }

    /// <summary>
    /// The session cookie is encrypted with keys the host's <see cref="IXmlRepository"/> keeps, so a restart does not sign every person
    /// out, and every instance over the same repository reads each other's cookies. The application name is fixed: the default is the
    /// content root, which differs between two hosts of the same coordinator.
    /// </summary>
    public static IServiceCollection AddSessionKeys(this IServiceCollection services)
    {
        services.AddDataProtection().SetApplicationName("dtpipe-coordinator");
        services.AddOptions<KeyManagementOptions>().Configure<IXmlRepository>((options, repository) => options.XmlRepository = repository);
        return services;
    }

    /// <summary>
    /// Reads the key ring now, creating a key when the repository holds none: a repository that cannot be reached stops the start, where
    /// the first request would otherwise find out and a key made then and lost would sign everyone out.
    /// </summary>
    public static void EnsureSessionKeys(IServiceProvider services) =>
        services.GetRequiredService<IDataProtectionProvider>().CreateProtector("session-keys-check").Protect("check");

    /// <summary>A person signed in, whatever their role: who they are is all <c>/api/me</c> needs.</summary>
    public const string SignedIn = "signed-in";

    /// <summary>
    /// A service the rights grant <see cref="IdentityKinds.Monitor"/> to: the only caller of <c>/health</c>. It is a service's token
    /// (<see cref="ServiceScheme"/>), and what it may do is the rights', so a node, an identity of another kind, an unknown or a
    /// disabled one is refused with its valid token.
    /// </summary>
    public const string Monitor = "monitor";

    /// <summary>
    /// A service the rights grant <see cref="IdentityKinds.Launcher"/> to: the only caller of <c>/service</c>. It is a service's token
    /// like a monitor's, and which pipelines it may launch is its identity's list, checked where the pipeline is named.
    /// </summary>
    public const string Launcher = "launcher";

    /// <summary>Why this server verifies no identity through a provider, or null when it does.</summary>
    public static string? ProviderUnavailable(CoordinatorServerOptions server) =>
        server.HasProvider ? null : "this coordinator trusts no identity provider (Coordinator:Authority is not set), so an identity is verified by a key";

    /// <summary>The email claim of the signed-in person, or null when there is none.</summary>
    public static string? EmailOf(HttpContext context) => context.User.FindFirst("email")?.Value;

    /// <summary>
    /// Who the request is from, as a run, an arming, a cursor and a rights change name their author: a person's email; else the id of
    /// the identity it is, a service or a person verified by a key, whose name is its <c>sub</c>, and an id is unique among the
    /// identities of every kind; else the name a provider gives a person with no email followed by <c> (idp)</c>, so that it is never
    /// an identity's id (a launcher of that id would otherwise take that person's runs for its own).
    /// </summary>
    public static string CallerOf(HttpContext context)
    {
        if (EmailOf(context) is { } email) return email;
        var user = context.User;
        if (user.Identity?.Name is not { } name) return "unknown";
        return IsService(user) || KeyPersonOf(user, context.RequestServices.GetRequiredService<CoordinatorServerOptions>()) is not null ? name : $"{name} (idp)";
    }

    /// <summary>
    /// The id and the key of the person verified by a key that <paramref name="user"/> is, or null for anyone else. Read from the claims
    /// the key issuer issued (<see cref="Claim.Issuer"/>), in its token or in the session it signed in, so a provider's token that names
    /// the same subject and carries <see cref="KeyIdClaim"/> is no such person.
    /// </summary>
    public static (string Id, string KeyId)? KeyPersonOf(ClaimsPrincipal? user, CoordinatorServerOptions server)
    {
        if (server.KeyIssuer is not { } issuer || user is null) return null;
        var id = user.FindFirst(c => c.Type == "sub" && c.Issuer == issuer)?.Value;
        var keyId = user.FindFirst(c => c.Type == KeyIdClaim && c.Issuer == issuer)?.Value;
        return id is { Length: > 0 } && keyId is { Length: > 0 } ? (id, keyId) : null;
    }

    /// <summary>The session a person verified by a key signs in with: its id and its key's, issued by the key issuer. Never the key.</summary>
    public static ClaimsPrincipal KeyPersonSession(CoordinatorServerOptions server, string id, string keyId) => new(new ClaimsIdentity(
        [new Claim("sub", id, ClaimValueTypes.String, server.KeyIssuer), new Claim(KeyIdClaim, keyId, ClaimValueTypes.String, server.KeyIssuer)],
        authenticationType: AuthModes.Key, nameType: "sub", roleType: ClaimsIdentity.DefaultRoleClaimType));

    /// <summary>Whether people may still be verified by a key here and the rights still hold <paramref name="person"/> signed in with its key.</summary>
    private static bool StillSignedIn(HttpContext context, CoordinatorServerOptions server, (string Id, string KeyId) person) =>
        server.PersonKeys
        && context.RequestServices.GetRequiredService<RightsStore>().SignedInPerson(person.Id, person.KeyId, DateTimeOffset.UtcNow) is not null;

    /// <summary>A page a browser opens: the only requests a missing session sends to sign in, at the form or the provider.</summary>
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

    /// <summary>
    /// What a service's scheme checks once a token is valid: it is a service's, and its issuer is the one that verifies the identity
    /// it names. The key issuer's token opens only an identity verified by a key; the provider's never opens one, and opens an
    /// identity it verifies or a subject the rights do not know (whose host waits for approval). Read on every request, so an identity
    /// removed and added again under another verification is judged as it is now.
    /// </summary>
    private static JwtBearerEvents ServiceEvents(bool byKey) => new()
    {
        // The node channel is SignalR, which may carry its token as the access_token query.
        OnMessageReceived = context =>
        {
            if (NodeChannelToken(context.Request) is { } token) context.Token = token;
            return Task.CompletedTask;
        },
        OnTokenValidated = context =>
        {
            if (!IsService(context.Principal))
            {
                context.Fail("Only a service's token reaches the data plane.");
                return Task.CompletedTask;
            }
            var identity = context.HttpContext.RequestServices.GetRequiredService<RightsStore>().Find(SubjectOf(context.Principal));
            var byKeyIdentity = identity?.Auth.Mode == AuthModes.Key;
            if (byKey && !byKeyIdentity) context.Fail("A token of the coordinator's key issuer opens only an identity verified by a key.");
            else if (!byKey && byKeyIdentity) context.Fail("An identity verified by a key is opened only by a token of the coordinator's key issuer.");
            return Task.CompletedTask;
        },
    };

    private static string? NodeChannelToken(HttpRequest request) =>
        request.Path.StartsWithSegments(DtPipe.Coordinator.Contracts.NodeChannel.Path) && request.Query["access_token"] is { Count: > 0 } token
            ? token.ToString()
            : null;

    /// <summary>The token a service's scheme reads, from where its bearer events read it.</summary>
    private static string? ServiceTokenOf(HttpRequest request) => NodeChannelToken(request) ?? BearerOf(request);

    /// <summary>The token of the request's <c>Authorization: Bearer</c> header, the only place a person's scheme reads one.</summary>
    private static string? BearerOf(HttpRequest request)
    {
        var header = request.Headers.Authorization.ToString();
        return header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? header["Bearer ".Length..].Trim() : null;
    }

    /// <summary>
    /// What the person scheme of the key issuer checks once a token is valid, on every request: it is a person's token obtained through
    /// the studio's client, and its subject is still an enabled person verified by a key whose key, named in the token, is live. A
    /// token lives <c>Coordinator:Auth:PersonTokenLifetime</c>; a revocation or a disablement applies at the next request.
    /// </summary>
    private static JwtBearerEvents KeyPersonEvents(CoordinatorServerOptions server) => new()
    {
        OnTokenValidated = context =>
        {
            if (!IsStudioPerson(context.Principal, server.StudioClientId))
                context.Fail("Only a person's token obtained through the studio's client reaches /api.");
            else if (KeyPersonOf(context.Principal, server) is not { } person || !StillSignedIn(context.HttpContext, server, person))
                context.Fail("The key this token was exchanged with no longer signs its person in.");
            return Task.CompletedTask;
        },
    };

    /// <summary>
    /// The issuer a token names, read without validating anything: it only chooses which scheme validates the token, in full, against
    /// that issuer's own keys, so a token that lies about its issuer fails there.
    /// </summary>
    private static string? IssuerOf(string? token)
    {
        if (string.IsNullOrEmpty(token)) return null;
        var handler = new JsonWebTokenHandler();
        if (!handler.CanReadToken(token)) return null;
        try { return handler.ReadJsonWebToken(token).Issuer; }
        catch (ArgumentException) { return null; }
    }
}

/// <summary>
/// A person's roles on this coordinator. A person of the provider is named by the claim <c>Coordinator:Roles:Claim</c> (<c>email</c> by
/// default), never by its <c>sub</c>, and takes the higher of two roles: the configuration's, where <c>Coordinator:Roles:Admin</c>,
/// <c>:Designer</c> and <c>:Reader</c> each list the people who hold it, separated by commas or semicolons, compared without case; and
/// the rights', where an enabled person of the provider has that value as its <see cref="Identity.Account"/>. A person verified by a key
/// (<see cref="TrustedIdp.KeyPersonOf"/>) takes the role of its identity in the rights, and never a list's.
/// Roles are cumulative (an admin is a designer, a designer a reader), and a person in no list and not in the rights holds
/// none: the provider or the key says who they are, never what they may do. Applied to every request, so a list
/// changed in configuration or a role changed in the rights takes effect without signing anyone out.
/// </summary>
public sealed class CoordinatorRoles(IConfiguration configuration, RightsStore rights, CoordinatorServerOptions server) : IClaimsTransformation
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

        IReadOnlyList<string> granted;
        if (TrustedIdp.KeyPersonOf(principal, server) is { } person)
            granted = Cumulative(rights.Find(person.Id) is { Enabled: true, IsPerson: true } held && held.Auth.Mode == AuthModes.Key ? held.Role : null);
        else
        {
            var who = principal.FindFirst(configuration["Coordinator:Roles:Claim"] ?? "email")?.Value?.Trim();
            if (string.IsNullOrEmpty(who)) return Task.FromResult(principal);

            bool InList(string key) => Listed(configuration, key).Contains(who, StringComparer.OrdinalIgnoreCase);
            var listed = InList("Admin") ? Admin : InList("Designer") ? Designer : InList("Reader") ? Reader : null;
            granted = Cumulative(Higher(listed, rights.ProviderPerson(who)?.Role));
        }
        var transformed = new ClaimsIdentity(identity);
        foreach (var role in granted)
            transformed.AddClaim(new Claim(transformed.RoleClaimType, role, ClaimValueTypes.String, Issuer));
        return Task.FromResult(new ClaimsPrincipal(transformed));
    }

    /// <summary>The people <c>Coordinator:Roles:{<paramref name="key"/>}</c> lists, separated by commas or semicolons.</summary>
    public static IReadOnlyList<string> Listed(IConfiguration configuration, string key) =>
        (configuration[$"Coordinator:Roles:{key}"] ?? "").Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>The higher of two roles, either of which may be none.</summary>
    private static string? Higher(string? one, string? other) =>
        new[] { Admin, Designer, Reader }.FirstOrDefault(role => role == one || role == other);

    /// <summary>The roles <paramref name="role"/> grants: itself and every role below it; none for no role.</summary>
    private static IReadOnlyList<string> Cumulative(string? role) => role switch
    {
        Admin => All,
        Designer => [Designer, Reader],
        Reader => [Reader],
        _ => [],
    };
}

/// <summary>The caller's identity in the rights is an enabled service of <see cref="Kind"/>.</summary>
public sealed class ServiceKindRequirement(string kind) : IAuthorizationRequirement
{
    public string Kind { get; } = kind;
}

/// <summary>
/// Grants <see cref="ServiceKindRequirement"/> by what the rights say of the token's subject, never by anything the token carries. The
/// token passed a service's scheme, so its issuer is the one that verifies that identity.
/// </summary>
public sealed class ServiceKindHandler(RightsStore rights) : AuthorizationHandler<ServiceKindRequirement>
{
    protected override Task HandleRequirementAsync(AuthorizationHandlerContext context, ServiceKindRequirement requirement)
    {
        if (rights.Find(TrustedIdp.SubjectOf(context.User)) is { Enabled: true } identity && identity.Kind == requirement.Kind) context.Succeed(requirement);
        return Task.CompletedTask;
    }
}

/// <summary>
/// TransportR's identity, with the groups the rights document gives the token's subject instead of
/// the token's own scopes. TransportR reads them at <c>Connect</c>, on every stream and at each token
/// renewal; an unknown or disabled subject resolves with no group, which TransportR refuses at
/// <c>Connect</c> and applies as a withdrawal at renewal. Everything else (client id, expiry,
/// subject) stays TransportR's own reading of the token. TransportR's routes, renewal included, take the
/// default policy, so the token passed a service's scheme and its issuer is the one that verifies that identity.
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
