using System.CommandLine;
using System.Net;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Commands;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The studio signed in to a coordinator with a key the coordinator made for the person, against a fake coordinator on the loopback:
/// the key is a reference read once, exchanged at the coordinator's own issuer and nowhere else, presented again to renew, and a
/// refusal signs the person out with its reason. Neither the key nor a token reaches a response or the page.
/// </summary>
[Collection("console-serial")]
public sealed class StudioKeyTests : IAsyncLifetime
{
    private const string Key = "dtp_alice_0123456789abcdefghijklmnopqrstuvwxyzABCDEFG";

    private readonly string _dir = Directory.CreateTempSubdirectory("studio-key-").FullName;
    private readonly StudioFakes _fakes = new() { Key = Key, Auth = [PersonAuth.Idp, PersonAuth.Key] };
    private readonly ManualTime _time = new();
    private readonly HttpClient _browser = new(new HttpClientHandler { AllowAutoRedirect = false });
    private CoordinatorLink _link = null!;
    private StudioServer _studio = null!;

    public async ValueTask InitializeAsync()
    {
        await _fakes.StartAsync();
        _link = new CoordinatorLink(new Uri(_fakes.CoordinatorUrl), _time, key: Key);
        _studio = await StudioServer.StartAsync(new StudioFolder(_dir), 0, new StudioTools(
            new FakeStudioEngine(), new DesignService(IBrickLookup.None), [], [], new InMemorySecretsManager(), _link), CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        await _studio.DisposeAsync();
        _link.Dispose();
        await _fakes.DisposeAsync();
        _browser.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<(HttpStatusCode Status, string Body)> StudioAsync(HttpMethod method, string path)
    {
        using var request = new HttpRequestMessage(method, $"http://127.0.0.1:{_studio.Port}{path}");
        request.Headers.Add(StudioServer.TokenHeader, _studio.Token);
        if (method == HttpMethod.Post) request.Content = new StringContent("{}", Encoding.UTF8, "application/json");
        using var response = await _browser.SendAsync(request, Ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(Ct));
    }

    [Fact]
    public async Task Signing_in_exchanges_the_key_at_the_coordinators_own_issuer_and_opens_no_tab()
    {
        var (status, body) = await StudioAsync(HttpMethod.Post, "/api/coordinator/login");

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(JsonValueKind.Null, JsonDocument.Parse(body).RootElement.GetProperty("url").ValueKind);
        Assert.Equal(new Dictionary<string, string> { ["grant_type"] = "client_credentials", ["client_id"] = "alice", ["client_secret"] = Key }, _fakes.LastKeyForm);
        Assert.Empty(_fakes.IdpPaths);
        var link = _link.Status();
        Assert.Equal((CoordinatorLink.SignedIn, true), (link.State, link.ByKey));
        var (_, shown) = await StudioAsync(HttpMethod.Get, "/api/coordinator");
        var (_, page) = await StudioAsync(HttpMethod.Get, "/");
        foreach (var text in new[] { body, shown, page })
        {
            Assert.DoesNotContain(Key[^20..], text);
            Assert.DoesNotContain("key-access-", text);
        }
        Assert.Contains("\"byKey\":true", shown);
    }

    [Fact]
    public async Task The_token_is_renewed_two_minutes_before_its_end_by_presenting_the_key_again()
    {
        await _link.SignInWithKeyAsync(Ct);
        _time.Now += TimeSpan.FromSeconds(_fakes.ExpiresIn) - CoordinatorLink.Margin - TimeSpan.FromSeconds(1);
        (await _link.SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: Ct)).Dispose();
        Assert.Equal(1, _fakes.KeyExchanges);

        _time.Now += TimeSpan.FromSeconds(2);
        using var renewed = await _link.SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: Ct);

        Assert.Equal(HttpStatusCode.OK, renewed.StatusCode);
        Assert.Equal(2, _fakes.KeyExchanges);
        Assert.Equal(Key, _fakes.LastKeyForm!["client_secret"]);
    }

    [Fact]
    public async Task A_refused_key_signs_the_person_out_with_the_coordinators_reason()
    {
        await _link.SignInWithKeyAsync(Ct);
        _fakes.RefuseKey = true;
        _time.Now += TimeSpan.FromSeconds(_fakes.ExpiresIn);

        var refused = await Assert.ThrowsAsync<CoordinatorLinkException>(() => _link.SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: Ct));

        var status = _link.Status();
        Assert.Equal(CoordinatorLink.SignedOut, status.State);
        Assert.Contains("its key does not open it", status.Reason);
        Assert.Equal(refused.Message, status.Reason);
        Assert.DoesNotContain(Key[^20..], status.Reason);
        Assert.Null(status.Me);
    }

    [Fact]
    public async Task A_coordinator_that_signs_nobody_in_with_a_key_is_refused_by_name()
    {
        _fakes.Auth = [PersonAuth.Idp];

        var refused = await Assert.ThrowsAsync<CoordinatorLinkException>(() => _link.SignInWithKeyAsync(Ct));

        Assert.Contains("signs nobody in with a key", refused.Message);
        Assert.Equal(0, _fakes.KeyExchanges);
    }

    [Fact]
    public async Task A_token_endpoint_off_the_coordinators_origin_never_receives_the_key()
    {
        _fakes.KeyTokenEndpoint = _fakes.IdpUrl + "/connect/token";

        var refused = await Assert.ThrowsAsync<CoordinatorLinkException>(() => _link.SignInWithKeyAsync(Ct));

        Assert.Contains("off the coordinator's origin", refused.Message);
        Assert.Equal(0, _fakes.KeyExchanges);
        Assert.DoesNotContain("/connect/token", _fakes.IdpPaths);
    }

    [Fact]
    public void A_text_that_is_no_key_is_refused_before_anything_is_sent()
    {
        var refused = Assert.Throws<CoordinatorLinkException>(() => new CoordinatorLink(new Uri(_fakes.CoordinatorUrl), key: "hunter2-secret"));

        Assert.DoesNotContain("hunter2", refused.Message);
        Assert.Equal(0, _fakes.KeyExchanges);
    }

    [Fact]
    public async Task Without_a_key_the_studio_never_starts_a_browser_sign_in_at_a_coordinator_of_keys_only()
    {
        _fakes.Auth = [PersonAuth.Key];
        using var browserLink = new CoordinatorLink(new Uri(_fakes.CoordinatorUrl), _time);

        var refused = await Assert.ThrowsAsync<CoordinatorLinkException>(() => browserLink.BeginSignInAsync(_studio.Port, Ct));

        Assert.Contains("--key", refused.Message);
    }

    [Fact]
    public async Task The_key_is_a_reference_read_once_and_never_a_value()
    {
        var secrets = new InMemorySecretsManager();
        secrets.SetSecret("studio-key", Key);
        var file = Path.Combine(_dir, "key.txt");
        await File.WriteAllTextAsync(file, Key + "\n", Ct);
        var variable = "DTPIPE_TEST_STUDIO_KEY_" + Guid.NewGuid().ToString("N");
        Environment.SetEnvironmentVariable(variable, Key);
        try
        {
            Assert.Equal((Key, null), await StudioCommand.ReadKeyAsync("${{" + variable + "}}", secrets, Ct));
            Assert.Equal((Key, null), await StudioCommand.ReadKeyAsync("@" + file, secrets, Ct));
            Assert.Equal((Key, null), await StudioCommand.ReadKeyAsync("keyring://studio-key", secrets, Ct));
        }
        finally
        {
            Environment.SetEnvironmentVariable(variable, null);
        }

        var (none, refusal) = await StudioCommand.ReadKeyAsync(Key, secrets, Ct);
        Assert.Null(none);
        Assert.Contains("never the key", refusal);
        Assert.DoesNotContain(Key, refusal);
    }

    [Theory]
    [InlineData("--coordinator", "http://127.0.0.1:9", "--key", Key, "never the key")]
    [InlineData("--key", "${{DTPIPE_TEST_STUDIO_KEY}}", null, null, "--coordinator")]
    public async Task The_command_refuses_a_key_given_as_a_value_or_without_a_coordinator(string option, string value, string? other, string? otherValue, string reason)
    {
        var services = new ServiceCollection().AddSingleton<ISecretsManager>(new InMemorySecretsManager()).BuildServiceProvider();
        string[] args = other is null ? ["studio", _dir, option, value] : ["studio", _dir, option, value, other, otherValue!];
        var error = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        int exit;
        try { exit = await new RootCommand { new StudioCommand(services) }.Parse(args).InvokeAsync(cancellationToken: Ct); }
        finally { Console.SetError(error); }

        Assert.Equal(1, exit);
        Assert.Contains(reason, captured.ToString());
        Assert.DoesNotContain(Key[^20..], captured.ToString());
    }
}
