using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Tests.Infrastructure;

/// <summary>
/// An OpenID Connect provider reduced to what a node host asks of one: a discovery document, and a client-credentials token
/// endpoint that signs, for the client that asks, a token a <see cref="TestJwt"/> hub accepts. The token's subject is the
/// <c>client_id</c> of the request.
/// </summary>
public sealed class FakeIdp : IAsyncDisposable
{
    private readonly WebApplication _app;

    public string Authority { get; }

    private FakeIdp(WebApplication app, string authority)
    {
        _app = app;
        Authority = authority;
    }

    public static async Task<FakeIdp> StartAsync(TestJwt jwt)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        var app = builder.Build();
        app.MapGet("/.well-known/openid-configuration", (HttpContext http) =>
            Results.Json(new { token_endpoint = $"{http.Request.Scheme}://{http.Request.Host}/token" }));
        app.MapPost("/token", async (HttpContext http) =>
        {
            var form = await http.Request.ReadFormAsync();
            return Results.Json(new { access_token = jwt.Mint(form["client_id"].ToString(), "unused", TimeSpan.FromHours(1)), expires_in = 3600 });
        });
        await app.StartAsync();
        return new FakeIdp(app, app.Urls.First());
    }

    public async ValueTask DisposeAsync() => await _app.DisposeAsync();
}
