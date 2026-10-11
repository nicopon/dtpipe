using System.Text.Json.Serialization;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.FlowControl;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

// The coordinator's server. Its settings are the Coordinator configuration section, from the
// environment first (Coordinator__Authority, Coordinator__StateDirectory, …); the pages are served
// from the wwwroot published beside the binary, whatever directory the server starts in.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.Logging.AddFilter("TransportR", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

var server = CoordinatorServerOptions.FromConfiguration(builder.Configuration);
builder.Services.AddSingleton(server);
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

// The coordinator as DtPipe.Coordinator ships it, in TransportR's production mode: every client
// is identified by a token of the trusted IdP, its group read from the rights document by its
// subject. The flow matrix is the rights document's too, read live by the hub on every transfer and
// by the planner on every plan.
builder.Services.AddSingleton<RightsStore>();
builder.Services.AddCoordinatorHub(_ => { }).Build();
builder.Services.AddNodeLink<NodeChannelHub>();
// A node answers a designer's check with a source brick's first row, larger than SignalR's 32 KB default.
builder.Services.Configure<HubOptions<NodeChannelHub>>(o => o.MaximumReceiveMessageSize = 1024 * 1024);
builder.Services.Replace(ServiceDescriptor.Singleton<IFlowControlService, RightsFlowControl>());
builder.Services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider, RightsIdentityProvider>());
builder.Services.AddTrustedIdp(server);

builder.Services.AddSingleton<LoggingHubProgressMonitor>();
builder.Services.Replace(ServiceDescriptor.Singleton<IHubProgressMonitor, EventProgressMonitor>());

builder.Services.AddSingleton<EventBus>();
builder.Services.AddSingleton<NodeInventory>();
builder.Services.AddSingleton<EnrollmentQueue>();
builder.Services.AddSingleton<NodeRequests>();
builder.Services.AddSingleton<BrickCatalog>();
builder.Services.AddSingleton<PipelineCatalog>();
builder.Services.AddSingleton<PlanBuilder>();
builder.Services.AddSingleton<AutoPlacer>();
builder.Services.AddSingleton<DesignValidator>();
builder.Services.AddSingleton(services => new PipelineLibrary(
    services.GetRequiredService<CoordinatorServerOptions>(), services.GetRequiredService<RightsStore>().ActiveGroupsOf));
builder.Services.AddSingleton<RunJournal>();
builder.Services.AddSingleton<DeploymentManager>();

var app = builder.Build();
app.Services.GetRequiredService<DeploymentManager>();

app.Use(async (context, next) =>
{
    try { await next(context); }
    catch (ConflictException ex)
    {
        context.Response.StatusCode = StatusCodes.Status409Conflict;
        await context.Response.WriteAsJsonAsync(new { error = ex.Message });
    }
});

// A 401 from /api names the document a client reads to learn how to sign in (RFC 9728).
app.Use((context, next) =>
{
    if (context.Request.Path.StartsWithSegments(CoordinatorApi.Prefix))
        context.Response.OnStarting(() =>
        {
            if (context.Response.StatusCode != StatusCodes.Status401Unauthorized) return Task.CompletedTask;
            var metadata = $"resource_metadata=\"{OriginOf(context)}{CoordinatorApi.Metadata}\"";
            var challenge = context.Response.Headers.WWWAuthenticate.ToString();
            context.Response.Headers.WWWAuthenticate = challenge.StartsWith("Bearer ", StringComparison.Ordinal)
                ? $"{challenge}, {metadata}"
                : $"Bearer {metadata}";
            return Task.CompletedTask;
        });
    return next(context);
});
app.UseAuthentication();
// The Lab view's pages exist only in lab mode, like the endpoints they call.
if (!server.LabMode)
    app.Use((context, next) => context.Request.Path.StartsWithSegments("/lab") ? Results.NotFound().ExecuteAsync(context) : next(context));
// A page is opened by a person signed in through the provider, with a role: the Lab view an admin's.
app.Use(async (context, next) =>
{
    if (!TrustedIdp.IsPageNavigation(context.Request) || context.Request.Path.StartsWithSegments("/account"))
    {
        await next(context);
        return;
    }
    if (context.User.Identity?.IsAuthenticated != true)
    {
        await context.ChallengeAsync(TrustedIdp.OidcScheme);
        return;
    }
    // A person with no role can open no page: they are signed out of the coordinator and of the
    // provider, so the next sign-in can be someone else's instead of the same refusal again.
    if (!context.User.IsInRole(CoordinatorRoles.Reader))
    {
        await Results.SignOut(new AuthenticationProperties { RedirectUri = "/account/denied" }, [TrustedIdp.CookieScheme, TrustedIdp.OidcScheme])
            .ExecuteAsync(context);
        return;
    }
    if (context.Request.Path.StartsWithSegments("/lab") && !context.User.IsInRole(CoordinatorRoles.Admin))
    {
        var who = System.Net.WebUtility.HtmlEncode(TrustedIdp.EmailOf(context) ?? context.User.Identity.Name ?? "This account");
        await AccessPage(StatusCodes.Status403Forbidden, $"{who} is not an admin of this coordinator: the Lab view is for administrators.",
            ("/", "Back to the coordinator"), ("/account/logout", "Sign out")).ExecuteAsync(context);
        return;
    }
    await next(context);
});
app.UseDefaultFiles();
app.UseStaticFiles();
app.UseRouting();
app.UseAuthorization();
app.MapDataHub(requireAuth: true);
app.MapHub<NodeChannelHub>(NodeChannel.Path).RequireAuthorization();

// Anonymous: whether the server answers at all, for a container's health check.
app.MapGet("/healthz", () => Results.Ok());
// Anonymous: where a person signs in for dtpipe studio, and the contract this server speaks.
app.MapGet(CoordinatorApi.Metadata, (HttpContext context, CoordinatorServerOptions options) =>
    new ResourceMetadata(OriginOf(context), [options.Authority], ["header"], CoordinatorApi.Contract, options.StudioClientId));

// A person signs in and out through the provider; the pages send them here.
app.MapGet("/account/login", (string? returnUrl) => Results.Challenge(
    new AuthenticationProperties { RedirectUri = returnUrl is { Length: > 0 } url && url.StartsWith('/') && !url.StartsWith("//") ? url : "/" },
    [TrustedIdp.OidcScheme]));
app.MapGet("/account/logout", () => Results.SignOut(new AuthenticationProperties { RedirectUri = "/" }, [TrustedIdp.CookieScheme, TrustedIdp.OidcScheme]));
// Where a person with no role lands once signed out: anonymous, it names nobody.
app.MapGet("/account/denied", () => AccessPage(StatusCodes.Status200OK,
    "That account holds no role on this coordinator, so it has been signed out, here and at the identity provider.",
    ("/", "Sign in with another account")));

// /api serves the pages by the session cookie, and dtpipe studio by a person's bearer token, with
// three cumulative roles: a reader sees everything, a designer also changes pipelines, deploys and
// runs them, an admin also changes the rights and, in lab mode, uses the bench's tools. A request that changes something carries the CSRF
// header. TransportR's own routes under /api (client, stream) are mapped by MapDataHub, not here.
var account = app.MapGroup("/api").RequireAuthorization(TrustedIdp.SignedIn).AddEndpointFilter(RequireCsrfHeader);
account.MapGet(CoordinatorApi.MeRoute, (HttpContext context) => new CoordinatorMe(
    context.User.Identity?.Name, context.User.FindFirst("email")?.Value, [.. CoordinatorRoles.All.Where(context.User.IsInRole)]));
account.MapGet("/info", (CoordinatorServerOptions options) => new { labMode = options.LabMode });

var api = app.MapGroup("/api").RequireAuthorization(CoordinatorRoles.Reader).AddEndpointFilter(RequireCsrfHeader);
var designer = api.MapGroup("").RequireAuthorization(CoordinatorRoles.Designer);
var admin = api.MapGroup("").RequireAuthorization(CoordinatorRoles.Admin);

api.MapNodes(designer);
api.MapLibrary(designer);
api.MapRights(admin);
api.MapRuns(designer);

if (server.LabMode) admin.MapLabMode();

app.Run();

static string OriginOf(HttpContext context) => $"{context.Request.Scheme}://{context.Request.Host}";

// A refused page navigation: a sentence and its ways out, readable without the pages' scripts.
// The message is HTML already encoded.
static IResult AccessPage(int status, string message, params (string Href, string Label)[] links) => Results.Content(
    "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
    + "<title>dtpipe coordinator</title><link rel=\"stylesheet\" href=\"/styles.css\"></head>"
    + "<body style=\"max-width:40rem;margin:4rem auto;padding:0 1rem;font-family:system-ui,sans-serif;line-height:1.5\">"
    + $"<h1 style=\"font-size:1.3rem\">dtpipe coordinator</h1><p>{message}</p><p>"
    + string.Join(" · ", links.Select(l => $"<a href=\"{l.Href}\">{l.Label}</a>"))
    + "</p></body></html>",
    "text/html; charset=utf-8",
    statusCode: status);

static async ValueTask<object?> RequireCsrfHeader(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
{
    var request = context.HttpContext.Request;
    if (HttpMethods.IsGet(request.Method) || HttpMethods.IsHead(request.Method) || request.Headers.ContainsKey(TrustedIdp.CsrfHeader))
        return await next(context);
    return Results.Problem($"A request that changes something carries the {TrustedIdp.CsrfHeader} header.", statusCode: StatusCodes.Status403Forbidden);
}
