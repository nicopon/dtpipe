using System.Security.Cryptography;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.Repositories;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>An <see cref="IXmlRepository"/> in memory, for the hosts of tests that are not about Redis.</summary>
public sealed class MemoryXmlRepository : IXmlRepository
{
    private readonly List<XElement> _elements = [];

    public IReadOnlyCollection<XElement> GetAllElements()
    {
        lock (_elements) return [.. _elements];
    }

    public void StoreElement(XElement element, string friendlyName)
    {
        lock (_elements) _elements.Add(element);
    }
}

/// <summary>The session cookie's keys live in the repository the host registers, and a repository that fails stops the start.</summary>
public sealed class SessionKeysTests
{
    private sealed class DownRepository : IXmlRepository
    {
        public IReadOnlyCollection<XElement> GetAllElements() => throw new StoreUnavailableException("down");

        public void StoreElement(XElement element, string friendlyName) => throw new StoreUnavailableException("down");
    }

    private static ServiceProvider Providers(IXmlRepository repository) =>
        new ServiceCollection().AddLogging().AddSessionKeys().AddSingleton(repository).BuildServiceProvider();

    [Fact]
    public void The_Keys_Are_Made_In_The_Repository_At_Start_And_Read_By_The_Next_Instance()
    {
        var repository = new MemoryXmlRepository();
        using var first = Providers(repository);
        TrustedIdp.EnsureSessionKeys(first);
        var cookie = first.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookie").Protect("session");

        Assert.NotEmpty(repository.GetAllElements());
        using var second = Providers(repository);
        Assert.Equal("session", second.GetRequiredService<IDataProtectionProvider>().CreateProtector("cookie").Unprotect(cookie));
    }

    [Fact]
    public void A_Repository_That_Cannot_Be_Reached_Stops_The_Start_Instead_Of_Minting_A_Key()
    {
        using var providers = Providers(new DownRepository());

        Assert.ThrowsAny<Exception>(() => TrustedIdp.EnsureSessionKeys(providers));
    }
}
