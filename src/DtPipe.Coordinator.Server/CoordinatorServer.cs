using System.Text.Json.Serialization;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Cursors;
using DtPipe.Coordinator.Server.Deployments;
using DtPipe.Coordinator.Server.Library;
using DtPipe.Coordinator.Server.Nodes;
using DtPipe.Coordinator.Server.Runs;
using DtPipe.Coordinator.Server.Stores;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TransportR.FlowControl;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The groups of <c>/api</c> a host maps its own routes on, behind every guard of the server: each requires its role, and a request
/// that changes something the CSRF header. The roles are cumulative: a designer is a reader, an admin a designer.
/// </summary>
public sealed record CoordinatorApiGroups(RouteGroupBuilder Reader, RouteGroupBuilder Designer, RouteGroupBuilder Admin);

/// <summary>
/// A view a host adds to the server's pages, under <paramref name="Path"/>: a person opens it signed in as any page, and only an admin
/// sees it; anyone else is shown a refusal that names it (<paramref name="Name"/>, as a sentence's subject).
/// </summary>
public sealed record AdminView(PathString Path, string Name);

/// <summary>The views the host added (<see cref="CoordinatorServer.AddCoordinatorServer"/>), which the page guard reads.</summary>
internal sealed class AdminViews(IReadOnlyList<AdminView> views)
{
    /// <summary>The view whose path holds <paramref name="path"/>, or null.</summary>
    public AdminView? Of(PathString path) => views.FirstOrDefault(v => path.StartsWithSegments(v.Path));
}

/// <summary>
/// The coordinator's server, added to a host. <see cref="AddCoordinatorServer"/> registers its services and its authentication;
/// <see cref="MapCoordinatorServerAsync"/> checks what a start needs, builds the request pipeline and maps the routes. The host owns
/// <c>Program</c>, its content root (the pages are the <c>wwwroot</c> published beside it) and its address; it maps its own routes
/// on the groups the map returns, and declares its own pages as admin views. The settings are the <c>Coordinator</c> configuration section.
/// </summary>
public static class CoordinatorServer
{
    /// <summary>
    /// Registers the server's services, from the <c>Coordinator</c> section of the builder's configuration, which it returns read, and the
    /// pages the host adds as <paramref name="adminViews"/>.
    /// </summary>
    public static CoordinatorServerOptions AddCoordinatorServer(this WebApplicationBuilder builder, params AdminView[] adminViews)
    {
        builder.Logging.AddFilter("TransportR", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

        var server = CoordinatorServerOptions.FromConfiguration(builder.Configuration);
        builder.Services.AddSingleton(server);
        builder.Services.AddSingleton(new AdminViews(adminViews));
        builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

        // The coordinator as DtPipe.Coordinator ships it, in TransportR's production mode: every client
        // is identified by a token of the trusted IdP, its group read from the rights document by its
        // subject. The flow matrix is the rights document's too, read live by the hub on every transfer and
        // by the planner on every plan.
        // The state the coordinator keeps beyond the process is in one database, a SQLite file, a PostgreSQL or a SQL Server database, which the stores borrow.
        builder.Services.AddCoordinatorStores(server);
        builder.Services.AddSingleton<RightsStore>();
        builder.Services.AddCoordinatorHub(_ => { }).Build();
        builder.Services.AddNodeLink<NodeChannelHub>();
        // A node answers a designer's check with a source brick's first row, larger than SignalR's 32 KB default.
        builder.Services.Configure<HubOptions<NodeChannelHub>>(o => o.MaximumReceiveMessageSize = 1024 * 1024);
        builder.Services.Replace(ServiceDescriptor.Singleton<IFlowControlService, RightsFlowControl>());
        builder.Services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider, RightsIdentityProvider>());
        builder.Services.AddTrustedIdp(server);
        if (server.KeyIssuer is not null) builder.Services.AddKeyIssuer();
        // The sign-in form of a person verified by a key carries an antiforgery token, checked by its handler.
        if (server.SignsPeopleInByKey) builder.Services.AddAntiforgery();

        builder.Services.AddSingleton<LoggingHubProgressMonitor>();
        builder.Services.Replace(ServiceDescriptor.Singleton<IHubProgressMonitor, EventProgressMonitor>());

        builder.Services.AddSingleton<EventBus>();
        builder.Services.AddSingleton<NodeInventory>();
        builder.Services.AddSingleton<EnrollmentQueue>();
        builder.Services.AddSingleton<NodeRequests>();
        builder.Services.AddSingleton<BrickCatalog>();
        builder.Services.AddSingleton<CursorKeeper>();
        builder.Services.AddSingleton<PlanBuilder>();
        builder.Services.AddSingleton<AutoPlacer>();
        builder.Services.AddSingleton<DesignValidator>();
        builder.Services.AddSingleton<BindingVerifier>();
        builder.Services.AddSingleton(services => new PipelineLibrary(
            services.GetRequiredService<CoordinatorServerOptions>(),
            services.GetRequiredService<AutoPlacer>().Check,
            services.GetRequiredService<ILoggerFactory>().CreateLogger<PipelineLibrary>()));
        builder.Services.AddSingleton<IJobSource>(services => services.GetRequiredService<PipelineLibrary>());
        builder.Services.AddSingleton<ILibraryHeads>(services => services.GetRequiredService<PipelineLibrary>());
        builder.Services.AddSingleton<RunJournal>();
        builder.Services.AddSingleton<OrphanRuns>();
        builder.Services.AddSingleton<DeploymentManager>();
        builder.Services.AddSingleton<LibraryArming>();
        builder.Services.AddSingleton<PipelineHealth>();
        return server;
    }

    /// <summary>
    /// Checks what a start needs (a failure stops it before anything is served), then builds the request pipeline and maps the routes.
    /// The host adds nothing to the pipeline after it: its routes go on the groups returned. A passive coordinator
    /// (<see cref="CoordinatorServerOptions.Passive"/>) first waits for the database, its <see cref="PassiveResponder"/> on the host's
    /// addresses, and returns once it holds it and has done the rest: the host's server then listens where the responder did.
    /// </summary>
    public static async Task<CoordinatorApiGroups> MapCoordinatorServerAsync(this WebApplication app)
    {
        if (!app.Services.GetRequiredService<CoordinatorServerOptions>().Passive) return await MapAsync(app);
        // The database is the first thing a standby opens, so nothing is read before it holds the lock; the responder answers until the
        // routes are mapped, and what no one answers is the host's own start, until its server listens.
        await using var responder = await PassiveResponder.StartAsync(app.Configuration);
        app.Services.GetRequiredService<SqlDatabase>();
        return await MapAsync(app);
    }

    private static async Task<CoordinatorApiGroups> MapAsync(WebApplication app)
    {
        var server = app.Services.GetRequiredService<CoordinatorServerOptions>();
        var adminViews = app.Services.GetRequiredService<AdminViews>();
        // The server's own category, whatever the host's name.
        var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger(typeof(CoordinatorServer).Namespace!);

        // The identities the deployment gives are added to the rights before anything is served, in one change. The log names their ids, never a key.
        var rights = app.Services.GetRequiredService<RightsStore>();
        var (bootstrapped, adminLeft) = rights.Bootstrap(server.Bootstrap);
        if (bootstrapped.Count > 0) logger.LogInformation("The rights gained {Identities} from Coordinator:Bootstrap", string.Join(", ", bootstrapped));
        if (adminLeft is not null)
            logger.LogWarning("Coordinator:Bootstrap:AdminKey names {Id}, an administrator of the rights with no live key: it is left as it is, and a key of another id adds an administrator", adminLeft);
        // An identity verified by a key with no issuer to exchange its key could never connect, nor a person while people may not hold a key,
        // nor an identity of the provider without one: the start says so before anything is served.
        KeyIssuer.CheckIdentities(rights, server);
        // First, so that every middleware and handler below sees the address the person's browser used: the sign-in's return address, the
        // protected-resource document and the cookies' Secure flag follow the request's scheme and host. No header is read.
        if (server.PublicUrl is { } publicUrl)
            app.Use((context, next) =>
            {
                context.Request.Scheme = publicUrl.Scheme;
                context.Request.Host = new HostString(publicUrl.Authority);
                return next(context);
            });
        // The session keys are read before anything is served: a store that is down stops the start instead of the first sign-in.
        TrustedIdp.EnsureSessionKeys(app.Services);
        // The issuer's signing keys are opened with the session keys, so they are read after them, and a store that cannot give them stops the start.
        if (server.KeyIssuer is not null) app.Services.GetRequiredService<IssuerKeys>();
        if (server.ExchangesKeysOverHttp)
            logger.LogWarning("Coordinator:Auth:AllowHttpKeys is set and Coordinator:PublicUrl is http: identity keys cross the network in clear at every token request.");
        // The armed deployments and the hosts set aside are read now too: a store that cannot be read stops the start.
        app.Services.GetRequiredService<DeploymentManager>();
        app.Services.GetRequiredService<EnrollmentQueue>();
        // With a remote, the library is read from it now: a remote that cannot be reached, or that holds nothing to begin from, stops the start.
        if (server.LibraryRemote is not null) await app.Services.GetRequiredService<PipelineLibrary>().OpenAsync(CancellationToken.None);

        app.Use(async (context, next) =>
        {
            try { await next(context); }
            catch (ConflictException ex)
            {
                context.Response.StatusCode = StatusCodes.Status409Conflict;
                await context.Response.WriteAsJsonAsync(new { error = ex.Message });
            }
            catch (StoreUnavailableException ex)
            {
                context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
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
        // A page is opened by a person signed in, with a role: an admin view an admin's. Where a person may sign in with a key, the sign-in
        // form offers both ways; otherwise the provider's sign-in begins at once.
        app.Use(async (context, next) =>
        {
            if (!TrustedIdp.IsPageNavigation(context.Request) || context.Request.Path.StartsWithSegments("/account"))
            {
                await next(context);
                return;
            }
            if (context.User.Identity?.IsAuthenticated != true)
            {
                if (server.SignsPeopleInByKey)
                    context.Response.Redirect($"{AccountPages.LoginPath}?returnUrl={Uri.EscapeDataString(context.Request.Path + context.Request.QueryString)}");
                else
                    await context.ChallengeAsync(TrustedIdp.OidcScheme);
                return;
            }
            // A person with no role can open no page: they are signed out of the coordinator and of the
            // provider, so the next sign-in can be someone else's instead of the same refusal again.
            if (!context.User.IsInRole(CoordinatorRoles.Reader))
            {
                await Results.SignOut(new AuthenticationProperties { RedirectUri = "/account/denied" }, SignedInWith(context.User)).ExecuteAsync(context);
                return;
            }
            if (adminViews.Of(context.Request.Path) is { } view && !context.User.IsInRole(CoordinatorRoles.Admin))
            {
                var who = System.Net.WebUtility.HtmlEncode(TrustedIdp.CallerOf(context));
                var what = System.Net.WebUtility.HtmlEncode(view.Name);
                await AccountPages.Access(server, StatusCodes.Status403Forbidden, $"{who} is not an admin of this coordinator: {what} is for administrators.",
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

        // Anonymous: whether the server serves, its store open (readiness), and whether the process answers at all (liveness), which a
        // passive standby answers too.
        app.MapGet("/healthz", () => Results.Ok());
        app.MapGet("/livez", () => Results.Ok());
        // Anonymous: where a person signs in for dtpipe studio, and the contract this server speaks.
        app.MapGet(CoordinatorApi.Metadata, (HttpContext context, CoordinatorServerOptions options) =>
            new ResourceMetadata(OriginOf(context), options.AuthorizationServers, ["header"], CoordinatorApi.Contract, options.StudioClientId, options.PersonAuth));

        // A person signs in through the provider, or with a key where people may hold one; the pages send them here. A person verified by a key
        // signs out here only: the provider never signed them in. Without a provider there is the form alone, which the start made sure of.
        IResult ProviderSignIn(string? returnUrl) =>
            Results.Challenge(new AuthenticationProperties { RedirectUri = AccountPages.LocalPath(returnUrl) }, [TrustedIdp.OidcScheme]);
        app.MapGet(AccountPages.LoginPath, (HttpContext context, string? returnUrl) => server.SignsPeopleInByKey
            ? AccountPages.LoginForm(context, server, AccountPages.LocalPath(returnUrl), null, StatusCodes.Status200OK)
            : ProviderSignIn(returnUrl));
        if (server.SignsPeopleInByKey)
        {
            if (server.HasProvider) app.MapGet(AccountPages.ProviderLoginPath, ProviderSignIn);
            app.MapPost(AccountPages.LoginPath, AccountPages.SignInAsync);
        }
        app.MapGet("/account/logout", (HttpContext context) => Results.SignOut(new AuthenticationProperties { RedirectUri = "/" }, SignedInWith(context.User)));
        // Where a person with no role lands once signed out: anonymous, it names nobody.
        app.MapGet("/account/denied", () => AccountPages.Access(server, StatusCodes.Status200OK,
            server.HasProvider
                ? "That account holds no role on this coordinator, so it has been signed out here, and at the identity provider when it signed in there."
                : "That key holds no role on this coordinator, so it has been signed out here.",
            ("/", "Sign in with another account")));

        // /api serves the pages by the session cookie, and dtpipe studio by a person's bearer token, with
        // three cumulative roles: a reader sees everything, a designer also changes pipelines, deploys and
        // runs them, an admin also changes the rights and uses what a host maps on the admin group. A request that changes something carries
        // the CSRF header. TransportR's own routes under /api (client, stream) are mapped by MapDataHub, not here.
        var account = app.MapGroup("/api").RequireAuthorization(TrustedIdp.SignedIn).AddEndpointFilter(RequireCsrfHeader);
        account.MapGet(CoordinatorApi.MeRoute, (HttpContext context) => new CoordinatorMe(
            context.User.Identity?.Name, context.User.FindFirst("email")?.Value, [.. CoordinatorRoles.All.Where(context.User.IsInRole)]));
        account.MapGet("/info", (CoordinatorServerOptions options) => new { httpKeys = options.ExchangesKeysOverHttp });

        var api = app.MapGroup("/api").RequireAuthorization(CoordinatorRoles.Reader).AddEndpointFilter(RequireCsrfHeader);
        var designer = api.MapGroup("").RequireAuthorization(CoordinatorRoles.Designer);
        var admin = api.MapGroup("").RequireAuthorization(CoordinatorRoles.Admin);

        api.MapNodes(designer);
        api.MapLibrary(designer);
        api.MapRights(admin);
        api.MapRuns(designer);
        app.MapHealth(api);
        app.MapService();

        if (server.KeyIssuer is not null) app.MapKeyIssuer(server);

        return new CoordinatorApiGroups(api, designer, admin);

        // What a person signs out of: the session, and the provider's too unless there is none or the session is a key's.
        string[] SignedInWith(System.Security.Claims.ClaimsPrincipal user) =>
            server.HasProvider && TrustedIdp.KeyPersonOf(user, server) is null ? [TrustedIdp.CookieScheme, TrustedIdp.OidcScheme] : [TrustedIdp.CookieScheme];
    }

    private static string OriginOf(HttpContext context) => $"{context.Request.Scheme}://{context.Request.Host}";

    private static async ValueTask<object?> RequireCsrfHeader(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        var request = context.HttpContext.Request;
        if (HttpMethods.IsGet(request.Method) || HttpMethods.IsHead(request.Method) || request.Headers.ContainsKey(TrustedIdp.CsrfHeader))
            return await next(context);
        return Results.Problem($"A request that changes something carries the {TrustedIdp.CsrfHeader} header.", statusCode: StatusCodes.Status403Forbidden);
    }
}
