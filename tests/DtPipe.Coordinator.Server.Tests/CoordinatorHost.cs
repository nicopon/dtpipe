using DtPipe.Jobs.Coordinator;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authentication.OpenIdConnect;
using Microsoft.AspNetCore.DataProtection.Repositories;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The coordinator's server in memory, trusting a test provider: its configuration is handed over
/// directly, with a key generated here, so nothing is discovered over the network. The state
/// directory is a fresh temporary one, and lab mode is off unless asked. <c>check</c> stands for the designer's check, which needs a runner and
/// the bricks' nodes; without it the real one runs. <c>publicUrl</c> is the address the server is told people reach it at.
/// </summary>
public sealed class CoordinatorHost(string? studioClientId = CoordinatorHost.StudioClient, Func<string, DesignCheck>? check = null, bool labMode = false,
    string? publicUrl = null)
    : WebApplicationFactory<Program>
{
    /// <summary>The bindings the stand-in check was last asked to judge, as the route merged them.</summary>
    public IReadOnlyDictionary<string, Binding>? CheckedBindings { get; private set; }

    public const string Issuer = "http://idp.test/";
    public const string Audience = "dtpipe-coordinator";
    public const string StudioClient = "studio";
    public const string Admin = "root@x.test";
    public const string Designer = "ann@x.test";
    public const string Reader = "rae@x.test";

    private static readonly RsaSecurityKey Key = new(RSA.Create(2048)) { KeyId = "test" };

    private readonly string _state = Path.Combine(Path.GetTempPath(), "coordinator-host-" + Guid.NewGuid().ToString("N"));

    /// <summary>The run journal's store; <c>Down</c> makes every call fail as an unreachable Redis does.</summary>
    internal FailingRunStore Runs => _runs ??= new FailingRunStore(MemoryRunStore.At(_state));

    /// <summary>The rights' store; <c>Down</c> makes every call fail as an unreachable Redis does.</summary>
    internal FailingRightsStore Rights => _rights ??= new FailingRightsStore(MemoryRightsStore.At(_state));

    /// <summary>The armed deployments' store; <c>Down</c> makes every call fail as an unreachable Redis does.</summary>
    internal FailingDeploymentStore Deployments => _deployments ??= new FailingDeploymentStore(MemoryDeploymentStore.At(_state));

    /// <summary>The store of the hosts set aside; <c>Down</c> makes every call fail as an unreachable Redis does.</summary>
    internal FailingIgnoredHostStore IgnoredHosts => _ignoredHosts ??= new FailingIgnoredHostStore(MemoryIgnoredHostStore.At(_state));

    private FailingRunStore? _runs;
    private FailingRightsStore? _rights;
    private FailingDeploymentStore? _deployments;
    private FailingIgnoredHostStore? _ignoredHosts;

    /// <summary>The server's state directory, which the host removes when it is disposed.</summary>
    public string StateDirectory => _state;

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("Coordinator:Authority", Issuer.TrimEnd('/'));
        builder.UseSetting("Coordinator:StateDirectory", _state);
        builder.UseSetting("Coordinator:Ui:ClientSecret", "ui-secret");
        builder.UseSetting("Coordinator:Roles:Admin", Admin);
        builder.UseSetting("Coordinator:Roles:Designer", Designer);
        builder.UseSetting("Coordinator:Roles:Reader", Reader);
        builder.UseSetting("Coordinator:Studio:ClientId", studioClientId ?? "");
        builder.UseSetting("Coordinator:LabMode", labMode ? "true" : "false");
        if (publicUrl is not null) builder.UseSetting("Coordinator:PublicUrl", publicUrl);
        builder.ConfigureTestServices(services =>
        {
            // What the coordinator keeps beyond the process is in memory: the real store is Redis, which these tests are not about.
            // Each can be taken down, as Redis can.
            services.AddSingleton<IRunStore>(Runs);
            services.AddSingleton<IRightsStore>(Rights);
            services.AddSingleton<IDeploymentStore>(Deployments);
            services.AddSingleton<IIgnoredHostStore>(IgnoredHosts);
            services.AddSingleton<IXmlRepository>(new MemoryXmlRepository());
            if (check is not null) services.AddSingleton<DesignValidator>(new StubValidator(check, bindings => CheckedBindings = bindings));
            var configuration = new OpenIdConnectConfiguration
            {
                Issuer = Issuer,
                AuthorizationEndpoint = Issuer + "connect/authorize",
                TokenEndpoint = Issuer + "connect/token",
                EndSessionEndpoint = Issuer + "connect/logout",
            };
            configuration.SigningKeys.Add(Key);
            services.ConfigureAll<JwtBearerOptions>(o => o.Configuration = configuration);
            services.ConfigureAll<OpenIdConnectOptions>(o => o.Configuration = configuration);
        });
    }

    /// <summary>A token signed by the test provider. <paramref name="audience"/> null leaves it without one.</summary>
    public static string Token(string subject, string? clientId, string? audience = Audience, string? email = null, string issuer = Issuer)
    {
        var claims = new Dictionary<string, object> { ["sub"] = subject };
        if (clientId is not null) claims["client_id"] = clientId;
        if (email is not null) claims["email"] = email;
        return new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Audience = audience,
            Claims = claims,
            Expires = DateTime.UtcNow.AddMinutes(10),
            SigningCredentials = new SigningCredentials(Key, SecurityAlgorithms.RsaSha256),
        });
    }

    /// <summary>A service's token: client credentials name the client as the subject.</summary>
    public static string Service(string clientId) => Token(clientId, clientId);

    /// <summary>A person's token, obtained through <paramref name="clientId"/>.</summary>
    public static string Person(string email, string clientId = StudioClient, string? audience = Audience) =>
        Token(email.Split('@')[0], clientId, audience, email);

    public HttpClient Anonymous() => CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

    public HttpClient Bearer(string token)
    {
        var client = Anonymous();
        client.DefaultRequestHeaders.Authorization = new("Bearer", token);
        return client;
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing && Directory.Exists(_state)) Directory.Delete(_state, recursive: true);
    }
}

/// <summary>The designer's check answered by a function, for tests that exercise what is around it.</summary>
internal sealed class StubValidator(Func<string, DesignCheck> check, Action<IReadOnlyDictionary<string, Binding>> seen) : DesignValidator(null!, null!)
{
    public override Task<DesignCheck> ValidateAsync(string yaml, IReadOnlyDictionary<string, Binding> bindings, CancellationToken ct)
    {
        seen(bindings);
        return Task.FromResult(check(yaml));
    }
}
