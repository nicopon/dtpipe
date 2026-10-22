namespace DtPipe.Coordinator.TrialIdp;

/// <summary>
/// What the trial identity provider needs to run inside any ASP.NET Core host: the stand-alone host
/// (<see cref="IdpOptions"/>) and the coordinator's trial mode build one each. Every name the provider
/// registers in its host (the session cookie, the login route) is a setting, so it never meets the
/// host's own.
/// </summary>
public sealed class TrialIdpSettings
{
    /// <summary>
    /// The issuer every token names. Browsers and services must reach the provider at this one address,
    /// or a token issued to one is refused by the other.
    /// </summary>
    public required Uri Issuer { get; init; }

    /// <summary>The audience of client-credentials tokens: the service they are meant for.</summary>
    public required string Audience { get; init; }

    /// <summary>The PEM file of the signing key, created on first start, so tokens survive a restart.</summary>
    public required string SigningKeyFile { get; init; }

    /// <summary>A user <c>u</c> is <c>u@EmailDomain</c>.</summary>
    public required string EmailDomain { get; init; }

    /// <summary>The authentication scheme and the cookie name of the provider's own session, apart from any session of its host.</summary>
    public required string CookieScheme { get; init; }
    public required string CookieName { get; init; }

    /// <summary>The route of the login form, in the host's address space.</summary>
    public required string LoginPath { get; init; }

    /// <summary>Whether a user name and password sign in.</summary>
    public required Func<string?, string?, bool> CheckUser { get; init; }

    /// <summary>The sentence of the login form that tells a person what to type.</summary>
    public required string LoginHint { get; init; }

    public TimeSpan AccessTokenLifetime { get; init; } = TimeSpan.FromHours(12);

    /// <summary>
    /// Origins a front end is also reached at: every redirect URI an authorization-code client registers is
    /// accepted at each of them too, path unchanged.
    /// </summary>
    public IReadOnlyList<Uri> ExtraOrigins { get; init; } = [];
}
