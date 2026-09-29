using System.Security.Claims;
using System.Security.Cryptography;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace DtPipe.PipelineNode.Tests.Infrastructure;

/// <summary>
/// The signing key of a production-mode <see cref="NodeTestHost"/>, and the tokens it accepts. A
/// token carries the subject and the <c>transportr:group:&lt;g&gt;</c> scope TransportR's
/// <c>JwtIdentityProvider</c> reads its client's identity from.
/// </summary>
public sealed class TestJwt
{
    private readonly SymmetricSecurityKey _key = new(RandomNumberGenerator.GetBytes(32));

    public TokenValidationParameters ValidationParameters => new()
    {
        ValidateIssuer = false,
        ValidateAudience = false,
        ValidateLifetime = true,
        IssuerSigningKey = _key,
    };

    public string Mint(string subject, string group, TimeSpan lifetime)
    {
        var descriptor = new SecurityTokenDescriptor
        {
            Subject = new ClaimsIdentity(
            [
                new Claim("sub", subject),
                new Claim("scope", "transportr:group:" + group),
            ]),
            Expires = DateTime.UtcNow.Add(lifetime),
            SigningCredentials = new SigningCredentials(_key, SecurityAlgorithms.HmacSha256),
        };
        return new JsonWebTokenHandler().CreateToken(descriptor);
    }
}
