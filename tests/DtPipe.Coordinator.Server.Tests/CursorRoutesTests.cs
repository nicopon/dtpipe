using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// Who may read and change a pipeline's cursors: whoever sees the pipeline reads the values, the right to launch it resets or sets one,
/// and a service reaches none of it (a token that is not a person's never opens <c>/api</c>, and <c>/service</c> has no cursor route).
/// </summary>
public sealed class CursorRoutesTests : IDisposable
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new();

    public void Dispose() => _host.Dispose();

    private HttpClient As(string email)
    {
        var client = _host.Bearer(CoordinatorHost.Person(email));
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        return client;
    }

    private void Keep() => _host.Services.GetRequiredService<CursorKeeper>().Set("sales", "snk", "integer", "42", "seed@x.test");

    [Fact]
    public async Task A_Reader_Reads_The_Values_And_Changes_Nothing()
    {
        Keep();
        using var reader = As(CoordinatorHost.Reader);

        using var read = await reader.GetAsync(CoordinatorApi.Cursors("sales"), Ct);
        using var reset = await reader.DeleteAsync(CoordinatorApi.Cursor("sales", "snk"), Ct);
        using var set = await reader.PutAsJsonAsync(CoordinatorApi.Cursor("sales", "snk"), new CursorSet("7"), Web, Ct);

        Assert.Equal(HttpStatusCode.OK, read.StatusCode);
        var cursor = (await read.Content.ReadFromJsonAsync<JsonElement>(Web, Ct))[0];
        Assert.Equal(("snk", "42"), (cursor.GetProperty("alias").GetString(), cursor.GetProperty("value").GetString()));
        Assert.Equal(HttpStatusCode.Forbidden, reset.StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, set.StatusCode);
        Assert.Equal("42", Assert.Single(_host.Services.GetRequiredService<CursorKeeper>().Of("sales")).Value);
    }

    [Fact]
    public async Task Whoever_May_Launch_The_Pipeline_Sets_A_Value_Inside_The_Rule_And_Resets_It()
    {
        Keep();
        using var designer = As(CoordinatorHost.Designer);

        using var refused = await designer.PutAsJsonAsync(CoordinatorApi.Cursor("sales", "snk"), new CursorSet("1'; drop table t; --"), Web, Ct);
        using var set = await designer.PutAsJsonAsync(CoordinatorApi.Cursor("sales", "snk"), new CursorSet("57"), Web, Ct);
        using var reset = await designer.DeleteAsync(CoordinatorApi.Cursor("sales", "snk"), Ct);
        using var again = await designer.DeleteAsync(CoordinatorApi.Cursor("sales", "snk"), Ct);

        Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
        Assert.Equal(HttpStatusCode.OK, set.StatusCode);
        var kept = await set.Content.ReadFromJsonAsync<JsonElement>(Web, Ct);
        Assert.Equal((CoordinatorHost.Designer, "57"), (kept.GetProperty("setBy").GetString(), kept.GetProperty("value").GetString()));
        Assert.Equal(HttpStatusCode.NoContent, reset.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, again.StatusCode);
        Assert.Empty(_host.Services.GetRequiredService<CursorKeeper>().Of("sales"));
    }

    [Fact]
    public async Task A_Value_Of_Another_Type_Than_The_Kept_One_Is_Refused_Until_The_Cursor_Is_Reset()
    {
        Keep();
        using var designer = As(CoordinatorHost.Designer);

        using var retyped = await designer.PutAsJsonAsync(CoordinatorApi.Cursor("sales", "snk"), new CursorSet("0 OR 1", "string"), Web, Ct);

        Assert.Equal(HttpStatusCode.Conflict, retyped.StatusCode);
        Assert.Contains("reset the cursor first", await retyped.Content.ReadAsStringAsync(Ct));
        Assert.Equal(("integer", "42"), (Assert.Single(_host.Services.GetRequiredService<CursorKeeper>().Of("sales")).Type, "42"));
        using var reset = await designer.DeleteAsync(CoordinatorApi.Cursor("sales", "snk"), Ct);
        using var again = await designer.PutAsJsonAsync(CoordinatorApi.Cursor("sales", "snk"), new CursorSet("abc", "string"), Web, Ct);
        Assert.Equal(HttpStatusCode.NoContent, reset.StatusCode);
        Assert.Equal(HttpStatusCode.OK, again.StatusCode);
    }

    [Fact]
    public async Task A_Service_Reaches_No_Cursor()
    {
        Keep();
        var rights = _host.Services.GetRequiredService<RightsStore>();
        rights.AddIdentity(new Identity("cron", "scheduler", "", [], Kind: IdentityKinds.Launcher, Pipelines: ["sales"]));
        using var launcher = _host.Bearer(CoordinatorHost.Service("cron"));

        using var api = await launcher.GetAsync(CoordinatorApi.Cursors("sales"), Ct);
        using var service = await launcher.GetAsync("/service/cursors", Ct);
        using var serviceRoute = await launcher.GetAsync("/service/library/sales/cursors", Ct);

        Assert.False(api.IsSuccessStatusCode);
        Assert.Equal(HttpStatusCode.NotFound, service.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, serviceRoute.StatusCode);
    }
}
