using System.Net;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The pages' files are served for a browser to revalidate: a copy kept from an earlier coordinator is never reused as it is,
/// or the page loads stylesheets and modules of two revisions.
/// </summary>
public sealed class StaticFileCachingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_Static_File_Is_Revalidated_By_Its_ETag_Before_A_Browser_Reuses_It()
    {
        using var host = new CoordinatorHost();
        var client = host.Anonymous();

        using var served = await client.GetAsync("/styles.css", Ct);
        Assert.Equal(HttpStatusCode.OK, served.StatusCode);
        Assert.Equal("no-cache", served.Headers.CacheControl?.ToString());
        var etag = Assert.IsType<System.Net.Http.Headers.EntityTagHeaderValue>(served.Headers.ETag);

        using var revalidation = new HttpRequestMessage(HttpMethod.Get, "/styles.css");
        revalidation.Headers.IfNoneMatch.Add(etag);
        using var unchanged = await client.SendAsync(revalidation, Ct);
        Assert.Equal(HttpStatusCode.NotModified, unchanged.StatusCode);
    }
}
