using System.Net;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;

namespace DtPipe.Coordinator.Server.Access;

/// <summary>
/// The pages of <c>/account</c> a browser opens without the coordinator's scripts: a refusal and its ways out, and the form a person
/// verified by a key signs in with. <c>Program.cs</c> maps them. A value a page shows is HTML-encoded first, and every page names an
/// exchange of keys over HTTP while it is in effect.
/// </summary>
internal static class AccountPages
{
    public const string LoginPath = "/account/login";

    /// <summary>The sign-in through the identity provider, beside the key form.</summary>
    public const string ProviderLoginPath = "/account/login/idp";

    /// <summary>The largest form the sign-in reads: a key, the return address and the antiforgery token.</summary>
    private const int MaxForm = 4096;

    /// <summary>A path of this server to go back to once signed in, or <c>/</c>: never another origin (<c>//host</c>, <c>/\host</c>).</summary>
    public static string LocalPath(string? returnUrl) =>
        returnUrl is { Length: > 0 } url && url[0] == '/' && (url.Length == 1 || url[1] is not ('/' or '\\')) ? url : "/";

    /// <summary>A refused page navigation: a sentence and its ways out. The message is HTML already encoded.</summary>
    public static IResult Access(CoordinatorServerOptions server, int status, string message, params (string Href, string Label)[] links) =>
        Page(server, status, $"<p>{message}</p><p>" + string.Join(" · ", links.Select(l => $"<a href=\"{l.Href}\">{l.Label}</a>")) + "</p>");

    /// <summary>
    /// The sign-in form: a key, and the way through the provider when there is one. The key goes in a field the browser neither shows
    /// nor fills, beside the form's antiforgery token, and no cache keeps the answer.
    /// </summary>
    public static IResult LoginForm(HttpContext http, CoordinatorServerOptions server, string returnUrl, string? refusal, int status)
    {
        http.Response.Headers.CacheControl = "no-store";
        http.Response.Headers.Pragma = "no-cache";
        var tokens = http.RequestServices.GetRequiredService<IAntiforgery>().GetAndStoreTokens(http);
        return Page(server, status,
            (refusal is null ? "" : $"<p role=\"alert\"><strong>{WebUtility.HtmlEncode(refusal)}</strong></p>")
            + $"<form method=\"post\" action=\"{LoginPath}\" autocomplete=\"off\">"
            + $"<input type=\"hidden\" name=\"{WebUtility.HtmlEncode(tokens.FormFieldName)}\" value=\"{WebUtility.HtmlEncode(tokens.RequestToken)}\">"
            + $"<input type=\"hidden\" name=\"returnUrl\" value=\"{WebUtility.HtmlEncode(returnUrl)}\">"
            + "<p><label>Your key<br><input type=\"password\" name=\"key\" required autocomplete=\"off\" spellcheck=\"false\" style=\"width:100%\"></label></p>"
            + "<p><button type=\"submit\">Sign in with a key</button></p></form>"
            + (server.HasProvider ? $"<p><a href=\"{ProviderLoginPath}?returnUrl={WebUtility.UrlEncode(returnUrl)}\">Sign in with the identity provider</a></p>" : ""));
    }

    /// <summary>
    /// A key for a session, mapped only where <see cref="CoordinatorServerOptions.SignsPeopleInByKey"/>, so over HTTPS or where HTTP is
    /// allowed. Every refusal of the key reads the same, whether it names no identity, is wrong, revoked or expired, opens a disabled
    /// identity or one that is no person; the form is checked as the issuer's token endpoint is (limited per source and per identity,
    /// bounded in size) and carries its antiforgery token. The session holds the identity's id and the key's
    /// (<see cref="TrustedIdp.KeyPersonSession"/>), never the key, and no log carries the key or a part of it.
    /// </summary>
    public static async Task<IResult> SignInAsync(
        HttpContext http, CoordinatorServerOptions server, RightsStore rights, KeyExchangeLimits limits, IAntiforgery antiforgery, ILogger<KeyExchangeLimits> logger)
    {
        var source = http.Connection.RemoteIpAddress?.ToString() ?? "";
        using (var lease = limits.FromSource(source))
            if (!lease.IsAcquired) return TooMany(http, server);
        if (!http.Request.HasFormContentType || http.Request.ContentLength is not (> 0 and <= MaxForm))
            return LoginForm(http, server, "/", "The sign-in form cannot be read: sign in again.", StatusCodes.Status400BadRequest);
        IFormCollection form;
        try
        {
            await antiforgery.ValidateRequestAsync(http);
            form = await http.Request.ReadFormAsync(http.RequestAborted);
        }
        catch (Exception ex) when (ex is AntiforgeryValidationException or InvalidDataException)
        {
            return LoginForm(http, server, "/", "The sign-in form has expired: sign in again.", StatusCodes.Status400BadRequest);
        }
        var returnUrl = LocalPath(form["returnUrl"]);
        var presented = form["key"].ToString();
        var named = ClearKey.ClientIdOf(presented);
        using (var lease = limits.ForIdentity(named ?? ""))
            if (!lease.IsAcquired) return TooMany(http, server);
        if (rights.KeyOf(presented, DateTimeOffset.UtcNow) is not ({ IsPerson: true } person, { } key))
        {
            logger.LogWarning("A sign-in with a key for {ClientId} from {Source} was refused", named ?? "a text that is no key", source);
            return LoginForm(http, server, returnUrl, "This key signs nobody in here.", StatusCodes.Status400BadRequest);
        }
        await http.SignInAsync(TrustedIdp.CookieScheme, TrustedIdp.KeyPersonSession(server, person.ClientId, key.Id));
        return Results.Redirect(returnUrl);
    }

    private static IResult TooMany(HttpContext http, CoordinatorServerOptions server)
    {
        http.Response.Headers.RetryAfter = "60";
        return LoginForm(http, server, "/", "Too many sign-ins: try again in a minute.", StatusCodes.Status429TooManyRequests);
    }

    private static IResult Page(CoordinatorServerOptions server, int status, string body)
    {
        var ver = CoordinatorServer.Version.Split('+')[0];
        return Results.Content(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
            + "<title>dtpipe coordinator</title><link rel=\"stylesheet\" href=\"/styles.css\"></head>"
            + "<body style=\"max-width:40rem;margin:4rem auto;padding:0 1rem;font-family:system-ui,sans-serif;line-height:1.5\">"
            + (server.ExchangesKeysOverHttp ? "<p style=\"background:#b00020;color:#fff;padding:.4rem .8rem;font-weight:600\">Identity keys cross the network in clear: this coordinator is reached over HTTP.</p>" : "")
            + $"<h1 style=\"font-size:1.3rem\">dtpipe coordinator <span style=\"font-size:.85rem;font-weight:400;color:var(--muted,#666);margin-left:.5rem\" title=\"v{CoordinatorServer.Version}\">v{ver}</span></h1>"
            + body
            + "</body></html>",
            "text/html; charset=utf-8",
            statusCode: status);
    }
}
