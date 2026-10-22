namespace DtPipe.Coordinator.TrialIdp;

/// <summary>
/// The trial identity provider as a host of its own, for a lab or a test bench that wants the provider apart from the coordinator
/// (the coordinator's trial mode runs the same provider inside itself). Never for production. An entry point of a named class, not top-level
/// statements: the coordinator references this assembly and has a <c>Program</c> of its own.
/// </summary>
public static class TrialIdpHost
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        var options = IdpOptions.From(builder.Configuration, builder.Environment.ContentRootPath);
        var clients = ClientsFileReader.Load(options.ClientsFile);
        builder.Logging.AddFilter("OpenIddict", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

        builder.Services.AddTrialIdp(options.Settings);
        builder.Services.AddAuthorization();
        builder.Services.AddSingleton<IHostedService>(services => new ClientsFileSeed(services.GetRequiredService<ClientRegistrar>(), clients));

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.UseAntiforgery();

        app.MapGet("/", () => Results.Text("dtpipe trial IdP, not for production: see /.well-known/openid-configuration"));
        app.MapGet("/healthz", () => Results.Ok());
        app.MapTrialIdp();

        app.Run();
    }
}
