namespace DtPipe.Lab.Idp;

/// <summary>
/// The test IdP's settings, from the <c>Idp</c> configuration section: environment variables
/// (<c>Idp__Issuer</c>, …), command-line arguments (<c>--Idp:Issuer</c>) or <c>appsettings.json</c>.
/// </summary>
public sealed class IdpOptions
{
    /// <summary>
    /// The issuer every token names. Browsers and services must reach the IdP at this one address,
    /// or a token issued to one is refused by the other.
    /// </summary>
    public required Uri Issuer { get; init; }

    /// <summary>The JSON file listing the clients: client credentials and authorization code.</summary>
    public required string ClientsFile { get; init; }

    /// <summary>The PEM file of the signing key, created on first start, so tokens survive a restart.</summary>
    public required string SigningKeyFile { get; init; }

    /// <summary>A user <c>u</c> is <c>u@EmailDomain</c>.</summary>
    public required string EmailDomain { get; init; }

    /// <summary>The audience of client-credentials tokens: the service they are meant for.</summary>
    public required string Audience { get; init; }

    public TimeSpan AccessTokenLifetime { get; init; } = TimeSpan.FromHours(12);

    public static IdpOptions From(IConfiguration configuration, string contentRoot)
    {
        var section = configuration.GetSection("Idp");
        var issuer = section["Issuer"] ?? "http://localhost:5190/";
        return new IdpOptions
        {
            // OpenIddict compares issuers as strings: one trailing slash, always.
            Issuer = new Uri(issuer.TrimEnd('/') + "/"),
            ClientsFile = Path.GetFullPath(section["ClientsFile"] ?? "clients.json", contentRoot),
            SigningKeyFile = Path.GetFullPath(section["SigningKeyFile"] ?? Path.Combine(".state", "signing-key.pem"), contentRoot),
            EmailDomain = section["EmailDomain"] ?? "dtpipe.test",
            Audience = section["Audience"] ?? "dtpipe-coordinator",
            AccessTokenLifetime = int.TryParse(section["AccessTokenLifetimeSeconds"], out var seconds) && seconds > 0
                ? TimeSpan.FromSeconds(seconds)
                : TimeSpan.FromHours(12),
        };
    }
}
