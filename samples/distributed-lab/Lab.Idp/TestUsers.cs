using System.Net;
using System.Security.Claims;
using System.Text.RegularExpressions;
using OpenIddict.Abstractions;
using static OpenIddict.Abstractions.OpenIddictConstants;

namespace DtPipe.Lab.Idp;

/// <summary>
/// The users of a test IdP: any name made of letters, digits, <c>.</c>, <c>_</c> or <c>-</c> is a
/// user, whose password is the name followed by <c>pwd</c> (<c>toto</c> / <c>totopwd</c>). A user is
/// a subject, a name and an email, nothing more: whatever a user may do is decided by the service
/// the user signs into.
/// </summary>
public static partial class TestUsers
{
    [GeneratedRegex("^[A-Za-z0-9._-]{1,64}$")]
    private static partial Regex ValidName();

    public static bool Check(string? user, string? password) =>
        user is not null && ValidName().IsMatch(user) && string.Equals(password, user + "pwd", StringComparison.Ordinal);

    /// <summary>
    /// The identity a signed-in user carries into OpenIddict's tokens. The name goes to the identity
    /// token under the <c>profile</c> scope, the email under <c>email</c>; the subject always.
    /// </summary>
    public static ClaimsIdentity Identity(string user, IdpOptions options, IEnumerable<string> scopes)
    {
        var identity = new ClaimsIdentity(OpenIddict.Server.AspNetCore.OpenIddictServerAspNetCoreDefaults.AuthenticationScheme, Claims.Name, Claims.Role);
        identity.SetClaim(Claims.Subject, user)
                .SetClaim(Claims.Name, user)
                .SetClaim(Claims.Email, $"{user}@{options.EmailDomain}");
        identity.SetScopes(scopes);
        identity.SetDestinations(claim => claim.Type switch
        {
            Claims.Subject => [Destinations.AccessToken, Destinations.IdentityToken],
            Claims.Name when identity.HasScope(Scopes.Profile) => [Destinations.AccessToken, Destinations.IdentityToken],
            Claims.Email when identity.HasScope(Scopes.Email) => [Destinations.AccessToken, Destinations.IdentityToken],
            _ => [Destinations.AccessToken],
        });
        return identity;
    }

    public static string LoginPage(string returnUrl, string antiforgeryField, string antiforgeryToken, string? user, string? error)
    {
        static string E(string? s) => WebUtility.HtmlEncode(s ?? "");
        var message = error is null ? "" : $"<p class=\"error\">{E(error)}</p>";
        return $$"""
            <!doctype html>
            <html lang="en">
            <head>
            <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
            <title>Lab IdP</title>
            <style>
              :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
              body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: Canvas; }
              form { width: min(22rem, calc(100vw - 2rem)); display: grid; gap: .75rem; }
              input, button { font: inherit; padding: .5rem .6rem; }
              .hint { opacity: .7; font-size: .9rem; } .error { color: #c62828; margin: 0; }
            </style>
            </head>
            <body>
            <form method="post" action="/account/login">
              <h1>Lab IdP</h1>
              <p class="hint">Test users only: the password of <code>toto</code> is <code>totopwd</code>.</p>
              {{message}}
              <input type="hidden" name="{{E(antiforgeryField)}}" value="{{E(antiforgeryToken)}}">
              <input type="hidden" name="returnUrl" value="{{E(returnUrl)}}">
              <label>User <input name="user" autocomplete="username" required autofocus value="{{E(user)}}"></label>
              <label>Password <input name="password" type="password" autocomplete="current-password" required></label>
              <button type="submit">Sign in</button>
            </form>
            </body>
            </html>
            """;
    }

    /// <summary>A return URL is followed only within this IdP: never an open redirect.</summary>
    public static bool IsLocal(string? url) =>
        !string.IsNullOrEmpty(url) && url[0] == '/' && (url.Length == 1 || (url[1] != '/' && url[1] != '\\'));
}
