using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A person of the identity provider in the rights: found by the account the provider gives it for <c>Coordinator:Roles:Claim</c>,
/// never by its <c>sub</c>, and holding the higher of the role its configuration lists and the role the rights give. The lists count as
/// administrators beside the rights, and a person of the provider with no email is never named as an identity's id.
/// </summary>
public sealed class ProviderPersonTests : IDisposable
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new();

    public void Dispose() => _host.Dispose();

    private static HttpClient Client(WebApplicationFactory<Program> app, string bearer)
    {
        var client = app.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });
        client.DefaultRequestHeaders.Authorization = new("Bearer", bearer);
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        return client;
    }

    private static HttpClient Admin(WebApplicationFactory<Program> app) => Client(app, CoordinatorHost.Person(CoordinatorHost.Admin));

    private static async Task<string[]> RolesAsync(WebApplicationFactory<Program> app, string token)
    {
        using var client = Client(app, token);
        return (await client.GetFromJsonAsync<CoordinatorMe>(CoordinatorApi.Me, Web, Ct))!.Roles.ToArray();
    }

    private static object Person(string id, string? account, string role) =>
        new { clientId = id, kind = IdentityKinds.Person, role, account, auth = AuthModes.Idp };

    [Fact]
    public async Task A_Person_Of_The_Provider_Is_Found_By_Its_Account_And_Takes_The_Higher_Role()
    {
        using var admin = Admin(_host);
        // Rae is listed a reader in the configuration; the rights make her an administrator, by her account in another case.
        (await admin.PostAsJsonAsync("/api/rights/identities", Person("rae", "RAE@x.test", CoordinatorRoles.Admin), Web, Ct)).EnsureSuccessStatusCode();
        (await admin.PostAsJsonAsync("/api/rights/identities", Person("zoe", "zoe@x.test", CoordinatorRoles.Designer), Web, Ct)).EnsureSuccessStatusCode();
        var rae = CoordinatorHost.Person(CoordinatorHost.Reader);

        Assert.Equal(CoordinatorRoles.All, await RolesAsync(_host, rae));
        Assert.Equal(new[] { CoordinatorRoles.Designer, CoordinatorRoles.Reader }, await RolesAsync(_host, CoordinatorHost.Person("zoe@x.test")));
        // The sub is no account: a token that names zoe's id with another email holds nothing of hers.
        Assert.Empty(await RolesAsync(_host, CoordinatorHost.Token("zoe", CoordinatorHost.StudioClient, email: "zed@x.test")));

        // The role the rights give changes at the next request, and the list's stays beneath it.
        (await admin.PutAsJsonAsync("/api/rights/identities/rae", new { role = CoordinatorRoles.Reader }, Web, Ct)).EnsureSuccessStatusCode();
        Assert.Equal(new[] { CoordinatorRoles.Reader }, await RolesAsync(_host, rae));
        (await admin.PutAsJsonAsync("/api/rights/identities/zoe", new { enabled = false }, Web, Ct)).EnsureSuccessStatusCode();
        Assert.Empty(await RolesAsync(_host, CoordinatorHost.Person("zoe@x.test")));

        var listed = (await admin.GetFromJsonAsync<JsonElement>("/api/rights", Ct)).GetProperty("identities").EnumerateArray().Single(i => i.GetProperty("clientId").GetString() == "rae");
        Assert.Equal("RAE@x.test", listed.GetProperty("account").GetString());
    }

    [Fact]
    public async Task A_Person_Of_The_Provider_Has_One_Account_Of_Its_Own_And_Is_Never_A_Key_s()
    {
        using var admin = Admin(_host);
        using (var none = await admin.PostAsJsonAsync("/api/rights/identities", Person("ann", null, CoordinatorRoles.Reader), Web, Ct))
        {
            Assert.Equal(HttpStatusCode.Conflict, none.StatusCode);
            Assert.Contains("with no account", await none.Content.ReadAsStringAsync(Ct));
        }
        (await admin.PostAsJsonAsync("/api/rights/identities", Person("ann", "ann@x.test", CoordinatorRoles.Reader), Web, Ct)).EnsureSuccessStatusCode();
        using (var twice = await admin.PostAsJsonAsync("/api/rights/identities", Person("ann-2", "Ann@X.test", CoordinatorRoles.Reader), Web, Ct))
        {
            Assert.Equal(HttpStatusCode.Conflict, twice.StatusCode);
            Assert.Contains("an account names one person", await twice.Content.ReadAsStringAsync(Ct));
        }
        // A person of the provider holds no key, and a key's person no account.
        using (var key = await admin.PostAsync("/api/rights/identities/ann/keys", JsonContent.Create(new { }), Ct))
            Assert.Equal(HttpStatusCode.Conflict, key.StatusCode);

        // People's keys turned off leave the provider's people alone.
        using var noKeys = new CoordinatorHost(settings: new Dictionary<string, string> { ["Auth:PersonKeys"] = "false" });
        using var noKeysAdmin = Admin(noKeys);
        (await noKeysAdmin.PostAsJsonAsync("/api/rights/identities", Person("ann", "ann@x.test", CoordinatorRoles.Reader), Web, Ct)).EnsureSuccessStatusCode();
        KeyIssuer.CheckIdentities(noKeys.Services.GetRequiredService<RightsStore>(), noKeys.Services.GetRequiredService<CoordinatorServerOptions>());
    }

    [Fact]
    public void The_Configuration_s_Administrators_Count_Beside_The_Rights()
    {
        var options = CoordinatorServerOptions.FromConfiguration(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Coordinator:Authority"] = "https://idp.example",
            ["Coordinator:Ui:ClientSecret"] = "s",
        }).Build());
        RightsStore Rights(string? listed)
        {
            var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["Coordinator:Roles:Admin"] = listed }).Build();
            var rights = new RightsStore(MemoryRightsStore.At(Guid.NewGuid().ToString("N")), options, configuration);
            rights.AddIdentity(new Identity("boss", "boss", "", []) { Kind = IdentityKinds.Person, Role = CoordinatorRoles.Admin, Account = "boss@x.test" });
            return rights;
        }

        Rights("root@x.test").RemoveIdentity("boss");
        var alone = Rights(" ; ");
        var refused = Assert.Throws<ConflictException>(() => alone.UpdateIdentity("boss", null, null, null, enabled: false));
        Assert.Contains("'boss' is the last administrator", refused.Message);
    }

    [Fact]
    public async Task A_Person_Of_The_Provider_With_No_Email_Is_Named_As_No_Identity_Is()
    {
        using var host = new CoordinatorHost(settings: new Dictionary<string, string> { ["Roles:Claim"] = "preferred_username", ["Roles:Admin"] = "zed" });
        var zed = CoordinatorHost.Token("zed", CoordinatorHost.StudioClient, extra: new Dictionary<string, object> { ["preferred_username"] = "zed", ["name"] = "zed" });
        using var client = Client(host, zed);

        (await client.PutAsJsonAsync("/api/rights/matrix", new { matrix = new Dictionary<string, string[]> { ["crm"] = ["dwh"] } }, Web, Ct)).EnsureSuccessStatusCode();

        Assert.Equal("zed (idp)", host.Services.GetRequiredService<RightsStore>().Snapshot().Audit![0].By);
    }
}
