using System.Security.Claims;
using DtPipe.Coordinator.TrialIdp;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OpenIddict.Abstractions;
using OpenIddict.Server.AspNetCore;
using static OpenIddict.Abstractions.OpenIddictConstants;

// A trial identity provider for the coordinator and its nodes, never for production: OpenIddict, client credentials for services and
// authorization code with PKCE for front ends, with refresh tokens for a client that asks for them. It says who a caller is; what a caller may do belongs
// to the services that trust it.
var builder = WebApplication.CreateBuilder(args);
var options = IdpOptions.From(builder.Configuration, builder.Environment.ContentRootPath);
builder.Services.AddSingleton(options);
var clients = ClientCatalog.Load(options.ClientsFile);
builder.Services.AddSingleton(clients);
builder.Logging.AddFilter("OpenIddict", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

builder.Services.AddDbContext<IdpDbContext>(o =>
{
    o.UseInMemoryDatabase("trial-idp");
    o.UseOpenIddict();
});

// The IdP's own session, the cookie the login form sets: the authorization endpoint reads it.
builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(o =>
    {
        o.Cookie.Name = "trial-idp";
        o.LoginPath = "/account/login";
        o.ExpireTimeSpan = TimeSpan.FromHours(8);
    });
builder.Services.AddAuthorization();
builder.Services.AddAntiforgery();

builder.Services.AddOpenIddict()
    .AddCore(o => o.UseEntityFrameworkCore().UseDbContext<IdpDbContext>())
    .AddServer(o =>
    {
        o.SetIssuer(options.Issuer);
        o.SetAuthorizationEndpointUris("connect/authorize")
         .SetTokenEndpointUris("connect/token")
         .SetEndSessionEndpointUris("connect/logout");
        o.AllowAuthorizationCodeFlow().AllowClientCredentialsFlow().AllowRefreshTokenFlow();
        o.RequireProofKeyForCodeExchange();
        o.RegisterScopes(Scopes.OpenId, Scopes.Email, Scopes.Profile, Scopes.OfflineAccess);
        // Access tokens are signed JWTs any service can validate against the published keys. The
        // encryption key only protects authorization codes, which do not need to outlive a restart.
        o.AddEphemeralEncryptionKey();
        o.AddSigningKey(PersistentSigningKey.LoadOrCreate(options.SigningKeyFile));
        o.DisableAccessTokenEncryption();
        o.SetAccessTokenLifetime(options.AccessTokenLifetime);
        o.UseAspNetCore()
         .EnableAuthorizationEndpointPassthrough()
         .EnableTokenEndpointPassthrough()
         .EnableEndSessionEndpointPassthrough()
         // Plain HTTP: a test IdP on a laptop or a compose network, never exposed.
         .DisableTransportSecurityRequirement();
    });
builder.Services.AddHostedService<ClientCatalog>();

var app = builder.Build();
app.UseAuthentication();
app.UseAuthorization();
app.UseAntiforgery();

app.MapGet("/", () => Results.Text("dtpipe trial IdP, not for production: see /.well-known/openid-configuration"));
app.MapGet("/healthz", () => Results.Ok());

app.MapGet("/account/login", (HttpContext context, IAntiforgery antiforgery, string? returnUrl) =>
    LoginPage(context, antiforgery, TestUsers.IsLocal(returnUrl) ? returnUrl! : "/", user: null, error: null));

app.MapPost("/account/login", async (HttpContext context, IAntiforgery antiforgery, [FromForm] string? user, [FromForm] string? password, [FromForm] string? returnUrl) =>
{
    var target = TestUsers.IsLocal(returnUrl) ? returnUrl! : "/";
    if (!TestUsers.Check(user, password))
        return LoginPage(context, antiforgery, target, user, "Unknown user or wrong password.");

    var session = new ClaimsIdentity(CookieAuthenticationDefaults.AuthenticationScheme, Claims.Name, Claims.Role);
    session.AddClaim(new Claim(Claims.Name, user!));
    await context.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(session));
    return Results.LocalRedirect(target);
});

app.MapMethods("/connect/authorize", [HttpMethods.Get, HttpMethods.Post], async (HttpContext context) =>
{
    var request = context.GetOpenIddictServerRequest() ?? throw new InvalidOperationException("Not an OpenID Connect request.");
    var session = await context.AuthenticateAsync(CookieAuthenticationDefaults.AuthenticationScheme);
    if (!session.Succeeded || session.Principal.Identity?.Name is not { } user)
    {
        if (request.HasPromptValue(PromptValues.None))
            return Forbid(Errors.LoginRequired, "The user is not signed in.");

        // Back here once signed in, with the same request.
        var parameters = context.Request.HasFormContentType ? [.. context.Request.Form] : context.Request.Query.ToList();
        return Results.Challenge(new AuthenticationProperties
        {
            RedirectUri = context.Request.PathBase + context.Request.Path + QueryString.Create(parameters),
        }, [CookieAuthenticationDefaults.AuthenticationScheme]);
    }

    var identity = TestUsers.Identity(user, options, request.GetScopes());
    // A client that names an audience gets access tokens that service accepts; any other client's
    // tokens name none, so no service takes them for its own.
    if (clients.FirstOrDefault(c => c.ClientId == request.ClientId)?.Audience is { } audience) identity.SetResources(audience);
    return Results.SignIn(new ClaimsPrincipal(identity), authenticationScheme: OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
});

app.MapPost("/connect/token", async (HttpContext context, IOpenIddictApplicationManager applications) =>
{
    var request = context.GetOpenIddictServerRequest() ?? throw new InvalidOperationException("Not an OpenID Connect request.");

    if (request.IsClientCredentialsGrantType())
    {
        // OpenIddict has already checked the client and its secret. The token names the client and is
        // meant for the configured audience; it carries no group and no scope beyond those asked for.
        var application = await applications.FindByClientIdAsync(request.ClientId!) ?? throw new InvalidOperationException("Unknown client.");
        var identity = new ClaimsIdentity(OpenIddictServerAspNetCoreDefaults.AuthenticationScheme, Claims.Name, Claims.Role);
        identity.SetClaim(Claims.Subject, request.ClientId)
                .SetClaim(Claims.Name, await applications.GetDisplayNameAsync(application));
        identity.SetScopes(request.GetScopes());
        identity.SetResources(options.Audience);
        identity.SetDestinations(_ => [Destinations.AccessToken]);
        return Results.SignIn(new ClaimsPrincipal(identity), authenticationScheme: OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
    }

    if (request.IsAuthorizationCodeGrantType() || request.IsRefreshTokenGrantType())
    {
        // The code or the refresh token carries the principal the authorization endpoint signed in,
        // destinations and audience included; a test user never changes, so it is issued again as it is.
        var grant = await context.AuthenticateAsync(OpenIddictServerAspNetCoreDefaults.AuthenticationScheme);
        return grant.Principal is { } principal
            ? Results.SignIn(principal, authenticationScheme: OpenIddictServerAspNetCoreDefaults.AuthenticationScheme)
            : Forbid(Errors.InvalidGrant, "The authorization code or refresh token is no longer valid.");
    }

    return Forbid(Errors.UnsupportedGrantType, "This grant type is not supported.");
});

app.MapMethods("/connect/logout", [HttpMethods.Get, HttpMethods.Post], async (HttpContext context) =>
{
    // OpenIddict has already checked post_logout_redirect_uri against the client's registration.
    await context.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
    return Results.SignOut(new AuthenticationProperties { RedirectUri = "/" }, [OpenIddictServerAspNetCoreDefaults.AuthenticationScheme]);
});

app.Run();

static IResult LoginPage(HttpContext context, IAntiforgery antiforgery, string returnUrl, string? user, string? error)
{
    var tokens = antiforgery.GetAndStoreTokens(context);
    return Results.Content(
        TestUsers.LoginPage(returnUrl, tokens.FormFieldName, tokens.RequestToken!, user, error),
        "text/html; charset=utf-8",
        statusCode: error is null ? StatusCodes.Status200OK : StatusCodes.Status401Unauthorized);
}

static IResult Forbid(string error, string description) => Results.Forbid(new AuthenticationProperties(new Dictionary<string, string?>
{
    [OpenIddictServerAspNetCoreConstants.Properties.Error] = error,
    [OpenIddictServerAspNetCoreConstants.Properties.ErrorDescription] = description,
}), [OpenIddictServerAspNetCoreDefaults.AuthenticationScheme]);
