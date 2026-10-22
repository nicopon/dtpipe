namespace DtPipe.Coordinator.TrialIdp;

/// <summary>
/// The stand-alone test IdP's settings, from the <c>Idp</c> configuration section: environment variables
/// (<c>Idp__Issuer</c>, …), command-line arguments (<c>--Idp:Issuer</c>) or <c>appsettings.json</c>.
/// </summary>
public sealed class IdpOptions
{
    /// <summary>The JSON file listing the clients: client credentials and authorization code.</summary>
    public required string ClientsFile { get; init; }

    public required TrialIdpSettings Settings { get; init; }

    public static IdpOptions From(IConfiguration configuration, string contentRoot)
    {
        var section = configuration.GetSection("Idp");
        var issuer = section["Issuer"] ?? "http://localhost:5190/";
        return new IdpOptions
        {
            ClientsFile = Path.GetFullPath(section["ClientsFile"] ?? "clients.json", contentRoot),
            Settings = new TrialIdpSettings
            {
                // OpenIddict compares issuers as strings: one trailing slash, always.
                Issuer = new Uri(issuer.TrimEnd('/') + "/"),
                SigningKeyFile = Path.GetFullPath(section["SigningKeyFile"] ?? Path.Combine(".state", "signing-key.pem"), contentRoot),
                EmailDomain = section["EmailDomain"] ?? "dtpipe.test",
                Audience = section["Audience"] ?? "dtpipe-coordinator",
                CookieScheme = "trial-idp",
                CookieName = "trial-idp",
                LoginPath = "/account/login",
                CheckUser = TestUsers.Check,
                LoginHint = "Trial IdP, not for production. Test users only: the password of toto is totopwd.",
                AccessTokenLifetime = int.TryParse(section["AccessTokenLifetimeSeconds"], out var seconds) && seconds > 0
                    ? TimeSpan.FromSeconds(seconds)
                    : TimeSpan.FromHours(12),
                ExtraOrigins = (section["ExtraOrigins"] ?? "")
                    .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .Select(origin => new Uri(origin))
                    .ToList(),
            },
        };
    }
}
