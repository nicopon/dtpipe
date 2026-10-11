using System.Net;
using System.Net.Http.Json;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The server's route table as the framework holds it: each endpoint with its methods, its pattern and what authorizes it, in
/// literals, so a route that moves, loses a policy or leaves its group changes a line here. The hubs and the routes
/// TransportR maps are in it: moving to another TransportR can change those lines, which is not a defect of the server. Lab
/// mode adds the bench's routes and nothing else. Every route that changes something answers 403, naming the CSRF header,
/// to a request without it.
/// </summary>
public sealed class RouteTableTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Every endpoint, one line each: <c>METHODS pattern [authorization]</c>, sorted.</summary>
    private static IReadOnlyList<string> TableOf(CoordinatorHost host)
    {
        using var _ = host.CreateClient();
        return host.Services.GetRequiredService<EndpointDataSource>().Endpoints.OfType<RouteEndpoint>()
            .Select(e =>
            {
                var methods = e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods is { Count: > 0 } m ? string.Join("+", m.Order()) : "*";
                var authorization = e.Metadata.GetOrderedMetadata<IAuthorizeData>()
                    .Select(a => a.Policy ?? (a.Roles is { Length: > 0 } roles ? "roles:" + roles : "default"))
                    .ToList();
                var anonymous = e.Metadata.GetMetadata<IAllowAnonymous>() is not null;
                return $"{methods} {e.RoutePattern.RawText} [{(anonymous ? "anonymous" : authorization.Count == 0 ? "none" : string.Join(" > ", authorization))}]";
            })
            .Order(StringComparer.Ordinal)
            .ToList();
    }

    private static readonly string[] Expected = [
        "* /hub/command [default]",
        "* /hub/command/negotiate [default]",
        "* /node-channel [default]",
        "* /node-channel/negotiate [default]",
        "DELETE /api/deployments/{id} [reader > designer]",
        "DELETE /api/library/{id} [reader > designer]",
        "DELETE /api/rights/identities/{id} [reader > admin]",
        "GET /.well-known/oauth-protected-resource [none]",
        "GET /account/denied [none]",
        "GET /account/login [none]",
        "GET /account/logout [none]",
        "GET /api/bricks [reader]",
        "GET /api/catalog [reader]",
        "GET /api/deployments [reader]",
        "GET /api/events [reader]",
        "GET /api/flow-matrix [reader]",
        "GET /api/info [signed-in]",
        "GET /api/library [reader]",
        "GET /api/library/{id} [reader]",
        "GET /api/library/{id}/at/{commit} [reader]",
        "GET /api/library/{id}/history [reader]",
        "GET /api/me [signed-in]",
        "GET /api/nodes [reader]",
        "GET /api/rights [reader]",
        "GET /api/runs [reader]",
        "GET /api/runs/queue [reader]",
        "GET /api/state [reader]",
        "GET /healthz [none]",
        "GET api/stream/{transferId}/receive [default]",
        "POST /api/bricks/recognize [reader]",
        "POST /api/bricks/{node}/{id}/preview [reader > designer]",
        "POST /api/bricks/{node}/{id}/sample [reader > designer]",
        "POST /api/library/{id}/deploy [reader > designer]",
        "POST /api/library/{id}/distribute [reader > designer]",
        "POST /api/rights/identities [reader > admin]",
        "POST /api/rights/identities/{id}/reconnect [reader > admin]",
        "POST /api/rights/pending/{subject}/accept [reader > admin]",
        "POST /api/rights/pending/{subject}/ignore [reader > admin]",
        "POST /api/runs [reader > designer]",
        "POST /api/runs/cancel [reader > designer]",
        "POST api/stream/{transferId}/send [default]",
        "PUT /api/library/{id} [reader > designer]",
        "PUT /api/rights/identities/{id} [reader > admin]",
        "PUT /api/rights/matrix [reader > admin]",
        "PUT api/client/{clientId:guid}/token [default]",
    ];

    private static readonly string[] LabOnly = [
        "GET /api/pipelines [reader > admin]",
        "POST /api/deploy [reader > admin]",
        "POST /api/fragments/{name}/kill [reader > admin]",
        "POST /api/plan [reader > admin]",
        "POST /api/query [reader > admin]",
    ];

    [Fact]
    public void The_Routes_Are_The_Ones_Listed_With_The_Authorization_Of_Each()
    {
        using var host = new CoordinatorHost();
        Assert.Equal(Expected, TableOf(host));
    }

    [Fact]
    public void Lab_Mode_Adds_The_Benchs_Routes_And_Nothing_Else()
    {
        using var host = new CoordinatorHost(labMode: true);
        Assert.Equal(Expected.Concat(LabOnly).Order(StringComparer.Ordinal), TableOf(host));
    }

    /// <summary>
    /// A request that changes something carries the CSRF header, whoever sends it: an admin's bearer with a body that binds
    /// is still refused without it, on every such route of <c>/api</c> (TransportR's own client and stream routes are not ours).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Every_Route_That_Changes_Something_Refuses_A_Request_Without_The_Csrf_Header(bool labMode)
    {
        using var host = new CoordinatorHost(labMode: labMode);
        var routes = TableOf(host)
            .Select(line => Regex.Match(line, @"^(?<methods>\S+) (?<pattern>/api/\S*) "))
            .Where(m => m.Success && !m.Groups["methods"].Value.Split('+').All(x => x is "GET" or "HEAD"))
            .Where(m => !m.Groups["pattern"].Value.StartsWith("/api/client") && !m.Groups["pattern"].Value.StartsWith("/api/stream"))
            .ToList();
        Assert.NotEmpty(routes);
        var client = host.Bearer(CoordinatorHost.Person(CoordinatorHost.Admin));

        foreach (var route in routes)
            foreach (var method in route.Groups["methods"].Value.Split('+').Where(x => x is not ("GET" or "HEAD")))
            {
                var path = Regex.Replace(route.Groups["pattern"].Value, @"\{[^}]+\}", "x");
                using var request = new HttpRequestMessage(new HttpMethod(method), path);
                if (method is "POST" or "PUT") request.Content = JsonContent.Create(new { });
                using var response = await client.SendAsync(request, Ct);
                Assert.True(response.StatusCode == HttpStatusCode.Forbidden, $"{method} {path} answered {(int)response.StatusCode}, not 403");
                Assert.Contains(TrustedIdp.CsrfHeader, await response.Content.ReadAsStringAsync(Ct));
            }
    }
}
