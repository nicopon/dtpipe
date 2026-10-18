using System.Security.Cryptography;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>The session cookie's keys over a real Redis (<see cref="RedisTestBed"/>), under a prefix of each test's own.</summary>
public sealed class RedisSessionKeysTests : IDisposable
{
    private readonly RedisTestBed _bed = new();

    public void Dispose() => _bed.Dispose();

    // One start of the coordinator: its own connection and its own data protection stack.
    private static ServiceProvider Start(RedisTestBed bed)
    {
        var providers = new ServiceCollection().AddLogging().AddSessionKeys()
            .AddSingleton<IXmlRepository>(new RedisXmlRepository(bed.Open())).BuildServiceProvider();
        TrustedIdp.EnsureSessionKeys(providers);
        return providers;
    }

    private static IDataProtector Cookie(ServiceProvider providers) => providers.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookie");

    [Fact]
    public void A_Cookie_One_Instance_Issued_Is_Read_By_The_Next_Instance_From_Redis_Alone()
    {
        RedisTestBed.Need();
        string cookie;
        using (var first = Start(_bed)) cookie = Cookie(first).Protect("session");

        using var next = Start(_bed);
        Assert.Equal("session", Cookie(next).Unprotect(cookie));
        // The keys come from Redis, not from this machine's profile: an instance on another prefix cannot read the cookie.
        Assert.True(_bed.Open().Db.ListLength(_bed.Prefix + "session-keys") > 0);
        using var otherBed = new RedisTestBed();
        using var other = Start(otherBed);
        Assert.Throws<CryptographicException>(() => Cookie(other).Unprotect(cookie));
    }
}
