using System.Security.Claims;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OpenIddict.Abstractions;
using OpenIddict.Server.AspNetCore;
using static OpenIddict.Abstractions.OpenIddictConstants;

namespace DtPipe.Coordinator.TrialIdp;

/// <summary>
/// A trial identity provider for the coordinator and its nodes, never for production: OpenIddict, client credentials for services and
/// authorization code with PKCE for front ends, with refresh tokens for a client that asks for them. It says who a caller is; what a
/// caller may do belongs to the services that trust it. It runs in any ASP.NET Core host: the stand-alone one, or the coordinator's trial
/// mode. A host that maps it also calls <c>UseAuthentication</c> (OpenIddict answers its discovery and key routes there) and
/// <c>UseAntiforgery</c> after its routing, for the login form; it hands the clients it wants to <see cref="ClientRegistrar"/>.
/// </summary>
public static class TrialIdpEndpoints
{
    public static IServiceCollection AddTrialIdp(this IServiceCollection services, TrialIdpSettings settings)
    {
        services.AddSingleton(settings);
        services.AddSingleton<ClientRegistrar>();
        services.AddDbContext<IdpDbContext>(o =>
        {
            o.UseInMemoryDatabase("trial-idp");
            o.UseOpenIddict();
        });

        // The provider's own session, the cookie the login form sets: the authorization endpoint reads it. No default scheme: the host's stand.
        services.AddAuthentication().AddCookie(settings.CookieScheme, o =>
        {
            o.Cookie.Name = settings.CookieName;
            o.LoginPath = settings.LoginPath;
            o.ExpireTimeSpan = TimeSpan.FromHours(8);
        });
        services.AddAntiforgery();

        services.AddOpenIddict()
            .AddCore(o => o.UseEntityFrameworkCore().UseDbContext<IdpDbContext>())
            .AddServer(o =>
            {
                o.SetIssuer(settings.Issuer);
                o.SetAuthorizationEndpointUris("connect/authorize")
                 .SetTokenEndpointUris("connect/token")
                 .SetEndSessionEndpointUris("connect/logout");
                o.AllowAuthorizationCodeFlow().AllowClientCredentialsFlow().AllowRefreshTokenFlow();
                o.RequireProofKeyForCodeExchange();
                o.RegisterScopes(Scopes.OpenId, Scopes.Email, Scopes.Profile, Scopes.OfflineAccess);
                // Access tokens are signed JWTs any service can validate against the published keys. The
                // encryption key only protects authorization codes, which do not need to outlive a restart.
                o.AddEphemeralEncryptionKey();
                o.AddSigningKey(PersistentSigningKey.LoadOrCreate(settings.SigningKeyFile));
                o.DisableAccessTokenEncryption();
                o.SetAccessTokenLifetime(settings.AccessTokenLifetime);
                o.UseAspNetCore()
                 .EnableAuthorizationEndpointPassthrough()
                 .EnableTokenEndpointPassthrough()
                 .EnableEndSessionEndpointPassthrough()
                 // Plain HTTP: a test IdP on a laptop or a compose network, never exposed.
                 .DisableTransportSecurityRequirement();
            });
        return services;
    }

    public static IEndpointRouteBuilder MapTrialIdp(this IEndpointRouteBuilder app)
    {
        var settings = app.ServiceProvider.GetRequiredService<TrialIdpSettings>();
        var registrar = app.ServiceProvider.GetRequiredService<ClientRegistrar>();
        var cookie = settings.CookieScheme;

        app.MapGet(settings.LoginPath, (HttpContext context, IAntiforgery antiforgery, string? returnUrl) =>
            LoginPage(context, antiforgery, settings, TestUsers.IsLocal(returnUrl) ? returnUrl! : "/", user: null, error: null));

        app.MapPost(settings.LoginPath, async (HttpContext context, IAntiforgery antiforgery, [FromForm] string? user, [FromForm] string? password, [FromForm] string? returnUrl) =>
        {
            var target = TestUsers.IsLocal(returnUrl) ? returnUrl! : "/";
            if (!settings.CheckUser(user, password))
                return LoginPage(context, antiforgery, settings, target, user, "Unknown user or wrong password.");

            var session = new ClaimsIdentity(cookie, Claims.Name, Claims.Role);
            session.AddClaim(new Claim(Claims.Name, user!));
            await context.SignInAsync(cookie, new ClaimsPrincipal(session));
            return Results.LocalRedirect(target);
        });

        app.MapMethods("/connect/authorize", [HttpMethods.Get, HttpMethods.Post], async (HttpContext context) =>
        {
            var request = context.GetOpenIddictServerRequest() ?? throw new InvalidOperationException("Not an OpenID Connect request.");
            var session = await context.AuthenticateAsync(cookie);
            if (!session.Succeeded || session.Principal.Identity?.Name is not { } user)
            {
                if (request.HasPromptValue(PromptValues.None))
                    return Forbid(Errors.LoginRequired, "The user is not signed in.");

                // Back here once signed in, with the same request.
                var parameters = context.Request.HasFormContentType ? [.. context.Request.Form] : context.Request.Query.ToList();
                return Results.Challenge(new AuthenticationProperties
                {
                    RedirectUri = context.Request.PathBase + context.Request.Path + QueryString.Create(parameters),
                }, [cookie]);
            }

            var identity = TestUsers.Identity(user, settings.EmailDomain, request.GetScopes());
            // A client that names an audience gets access tokens that service accepts; any other client's
            // tokens name none, so no service takes them for its own.
            if (registrar.Current.FirstOrDefault(c => c.ClientId == request.ClientId)?.Audience is { } audience) identity.SetResources(audience);
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
                identity.SetResources(settings.Audience);
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
            await context.SignOutAsync(cookie);
            return Results.SignOut(new AuthenticationProperties { RedirectUri = "/" }, [OpenIddictServerAspNetCoreDefaults.AuthenticationScheme]);
        });

        return app;
    }

    private static IResult LoginPage(HttpContext context, IAntiforgery antiforgery, TrialIdpSettings settings, string returnUrl, string? user, string? error)
    {
        var tokens = antiforgery.GetAndStoreTokens(context);
        return Results.Content(
            TestUsers.LoginPage(settings, returnUrl, tokens.FormFieldName, tokens.RequestToken!, user, error),
            "text/html; charset=utf-8",
            statusCode: error is null ? StatusCodes.Status200OK : StatusCodes.Status401Unauthorized);
    }

    private static IResult Forbid(string error, string description) => Results.Forbid(new AuthenticationProperties(new Dictionary<string, string?>
    {
        [OpenIddictServerAspNetCoreConstants.Properties.Error] = error,
        [OpenIddictServerAspNetCoreConstants.Properties.ErrorDescription] = description,
    }), [OpenIddictServerAspNetCoreDefaults.AuthenticationScheme]);
}
