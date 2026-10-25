namespace DtPipe.Coordinator.Server.Stores;

/// <summary>
/// What a passive coordinator (<c>Coordinator:Standby</c>) serves while it waits for the database, on the addresses the host's
/// configuration gives the host: <c>/livez</c> 200, <c>/healthz</c> 503 <c>passive</c>, anything else 503 with <c>Retry-After</c>,
/// SignalR's negotiation included, so that a client sent here learns to go elsewhere or to come back. It is a server of its own over
/// the host's configuration, and reads none of the coordinator's services: it never opens the store, the library or the session keys.
/// It has no lifetime of its own, so a stop signal while it serves ends the process as the runtime ends it (exit code 143 on
/// <c>SIGTERM</c>), nothing taken. Disposed, it stops listening, and the host's server takes the addresses once the start is done.
/// </summary>
internal sealed class PassiveResponder : IAsyncDisposable
{
    /// <summary>What <c>/healthz</c> answers, and with it every other answer's text.</summary>
    public const string State = "passive";

    /// <summary>The seconds a refused client is told to wait: what a node waits between its tries anyway, a few seconds.</summary>
    public const int RetryAfterSeconds = 5;

    private readonly WebApplication _app;

    private PassiveResponder(WebApplication app) => _app = app;

    /// <summary>Listens where the host's <paramref name="configuration"/> says the host will.</summary>
    public static async Task<PassiveResponder> StartAsync(IConfiguration configuration)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Configuration.AddConfiguration(configuration);
        // A probe every few seconds would otherwise log two lines each.
        builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
        builder.Services.AddSingleton<IHostLifetime, NoLifetime>();
        var app = builder.Build();
        app.Run(context =>
        {
            var path = context.Request.Path;
            if (path == "/livez") return Task.CompletedTask;
            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            context.Response.ContentType = "text/plain";
            if (path == "/healthz") return context.Response.WriteAsync(State);
            context.Response.Headers.RetryAfter = RetryAfterSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture);
            return context.Response.WriteAsync($"This coordinator is {State}: another holds its database.");
        });
        await app.StartAsync();
        return new PassiveResponder(app);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    // No signal handler: the default lifetime would take the stop signal for this server alone and leave the wait running.
    private sealed class NoLifetime : IHostLifetime
    {
        public Task WaitForStartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
