using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using DtPipe.Cli.Security;
using DtPipe.Cli.Studio;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// The studio signed in to a coordinator, against a fake identity provider and a fake coordinator on
/// the loopback, built on the records the real coordinator writes: the sign-in, its single-use state,
/// where the tokens go and where they never go, and their renewal.
/// </summary>
public sealed class StudioCoordinatorTests : IAsyncLifetime
{
    private readonly string _dir = Directory.CreateTempSubdirectory("studio-link-").FullName;
    private readonly StudioFakes _fakes = new();
    private readonly ManualTime _time = new();
    private readonly HttpClient _browser = new(new HttpClientHandler { AllowAutoRedirect = false });
    private CoordinatorLink _link = null!;
    private StudioServer _studio = null!;

    public async ValueTask InitializeAsync()
    {
        await _fakes.StartAsync();
        _link = new CoordinatorLink(new Uri(_fakes.CoordinatorUrl), _time);
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

    /// <summary>The page asks to sign in; the browser follows the provider, which answers at once.</summary>
    private async Task<string> CallbackAddressAsync()
    {
        var (status, body) = await StudioAsync(HttpMethod.Post, "/api/coordinator/login");
        Assert.Equal(HttpStatusCode.OK, status);
        var authorize = JsonDocument.Parse(body).RootElement.GetProperty("url").GetString()!;
        Assert.StartsWith(_fakes.IdpUrl + "/authorize?", authorize);
        using var answer = await _browser.GetAsync(authorize, Ct);
        return answer.Headers.Location!.ToString();
    }

    private async Task<(HttpStatusCode Status, string Body)> BrowseAsync(string url)
    {
        using var response = await _browser.GetAsync(url, Ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(Ct));
    }

    private async Task SignInAsync()
    {
        var (status, body) = await BrowseAsync(await CallbackAddressAsync());
        Assert.True(status == HttpStatusCode.OK, body);
    }

    [Fact]
    public async Task Signing_in_exchanges_the_code_with_the_verifier_of_its_challenge_at_the_loopback()
    {
        var callback = await CallbackAddressAsync();
        Assert.StartsWith($"http://127.0.0.1:{_studio.Port}/callback?", callback);

        var (status, body) = await BrowseAsync(callback);

        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Contains("ann@x.test", body);
        var link = _link.Status();
        Assert.Equal((CoordinatorLink.SignedIn, "ann@x.test"), (link.State, link.Me?.Email));
        Assert.Equal("1", _fakes.LastCsrf);
    }

    [Fact]
    public async Task A_state_used_once_is_refused_the_second_time()
    {
        var callback = await CallbackAddressAsync();
        Assert.Equal(HttpStatusCode.OK, (await BrowseAsync(callback)).Status);

        var (status, body) = await BrowseAsync(callback);

        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("already used", body);
    }

    [Fact]
    public async Task An_unknown_state_is_refused_and_leaves_the_waiting_sign_in_alone()
    {
        var callback = await CallbackAddressAsync();
        var forged = System.Text.RegularExpressions.Regex.Replace(callback, "state=[^&]+", "state=forged");

        Assert.Equal(HttpStatusCode.BadRequest, (await BrowseAsync(forged)).Status);
        Assert.Equal(HttpStatusCode.OK, (await BrowseAsync(callback)).Status);
    }

    [Fact]
    public async Task A_sign_in_that_comes_back_too_late_is_refused()
    {
        var callback = await CallbackAddressAsync();
        _time.Now += CoordinatorLink.PendingLifetime + TimeSpan.FromSeconds(1);

        var (status, body) = await BrowseAsync(callback);

        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("too long", body);
        Assert.Equal(CoordinatorLink.SignedOut, _link.Status().State);
    }

    [Fact]
    public async Task An_answer_naming_another_issuer_is_refused()
    {
        var callback = await CallbackAddressAsync();
        var (status, body) = await BrowseAsync(System.Text.RegularExpressions.Regex.Replace(callback, "iss=[^&]+", "iss=" + Uri.EscapeDataString("http://evil.test/")));

        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("another identity provider", body);
        Assert.Equal(CoordinatorLink.SignedOut, _link.Status().State);
    }

    [Fact]
    public async Task No_token_reaches_a_response_or_the_page()
    {
        var (_, callbackPage) = await BrowseAsync(await CallbackAddressAsync());
        var (_, status) = await StudioAsync(HttpMethod.Get, "/api/coordinator");
        var (_, page) = await BrowseAsync($"http://127.0.0.1:{_studio.Port}/");

        foreach (var text in new[] { callbackPage, status, page })
        {
            Assert.DoesNotContain("access-", text);
            Assert.DoesNotContain("refresh-", text);
            Assert.DoesNotContain("code-1", text);
        }
        Assert.Contains(CoordinatorLink.SignedIn, status);
    }

    [Fact]
    public async Task A_redirect_to_another_origin_is_returned_never_followed()
    {
        await SignInAsync();

        using var response = await _link.SendAsync(HttpMethod.Get, "/api/elsewhere", ct: Ct);

        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.DoesNotContain("/stolen", _fakes.IdpPaths);
    }

    [Fact]
    public async Task The_access_token_is_renewed_ahead_of_its_expiry_and_a_rotated_refresh_token_replaces_the_spent_one()
    {
        await SignInAsync();
        _time.Now += TimeSpan.FromSeconds(_fakes.ExpiresIn) - CoordinatorLink.Margin - TimeSpan.FromSeconds(1);
        (await _link.SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: Ct)).Dispose();
        Assert.Equal(0, _fakes.Refreshes);

        _time.Now += TimeSpan.FromSeconds(2);
        using (var renewed = await _link.SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: Ct))
            Assert.Equal(HttpStatusCode.OK, renewed.StatusCode);
        Assert.Equal(1, _fakes.Refreshes);

        // The provider accepts only the refresh token it sent last.
        _time.Now += TimeSpan.FromSeconds(_fakes.ExpiresIn);
        using (var again = await _link.SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: Ct))
            Assert.Equal(HttpStatusCode.OK, again.StatusCode);
        Assert.Equal(2, _fakes.Refreshes);
    }

    [Fact]
    public async Task A_refused_renewal_signs_the_person_out_with_the_reason()
    {
        await SignInAsync();
        _fakes.RefuseRefresh = true;
        _time.Now += TimeSpan.FromSeconds(_fakes.ExpiresIn);

        var refused = await Assert.ThrowsAsync<CoordinatorLinkException>(() => _link.SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: Ct));

        var status = _link.Status();
        Assert.Equal(CoordinatorLink.SignedOut, status.State);
        Assert.Contains("sign in again", status.Reason);
        Assert.Equal(refused.Message, status.Reason);
        Assert.Null(status.Me);
    }

    [Fact]
    public async Task A_401_renews_the_token_once_and_sends_again_once()
    {
        await SignInAsync();
        _fakes.ValidAccess = "revoked";
        var before = _fakes.MeCalls;

        using var response = await _link.SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal((1, before + 2), (_fakes.Refreshes, _fakes.MeCalls));
    }

    [Fact]
    public async Task A_coordinator_speaking_another_contract_is_refused_by_name()
    {
        _fakes.Contract = CoordinatorApi.Contract + 98;

        var (status, body) = await StudioAsync(HttpMethod.Post, "/api/coordinator/login");

        Assert.Equal(HttpStatusCode.BadGateway, status);
        Assert.Contains($"contract {CoordinatorApi.Contract + 98}", body);
        Assert.Contains($"contract {CoordinatorApi.Contract}", body);
    }

    [Fact]
    public async Task A_provider_whose_document_names_another_issuer_is_refused()
    {
        _fakes.IssuerAnswered = "http://elsewhere.test/";

        var (status, body) = await StudioAsync(HttpMethod.Post, "/api/coordinator/login");

        Assert.Equal(HttpStatusCode.BadGateway, status);
        Assert.Contains("names itself http://elsewhere.test/", body);
    }

    [Fact]
    public async Task The_callback_answers_only_at_the_loopback()
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"http://127.0.0.1:{_studio.Port}/callback?code=x&state=y");
        request.Headers.Host = $"evil.example:{_studio.Port}";
        using var response = await _browser.SendAsync(request, Ct);
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }
}
