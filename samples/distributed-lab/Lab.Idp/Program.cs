namespace DtPipe.Lab.Idp;

/// <summary>
/// The lab's identity provider as a host of its own, an external provider on an origin apart from the coordinator's. Never for
/// production.
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
