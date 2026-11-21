using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// <c>Coordinator:DataPlane:RequiredProtection</c> is one of the words of the protection of the data plane, none when it is absent, and
/// what admission reads; any other text stops the start, naming the setting and the words, and not what was given.
/// </summary>
public sealed class RequiredProtectionSettingTests
{
    private static CoordinatorServerOptions Options(string? required)
    {
        var settings = new Dictionary<string, string?>
        {
            ["Coordinator:Authority"] = "http://idp", ["Coordinator:Ui:ClientSecret"] = "x", ["Coordinator:DataPlane:RequiredProtection"] = required,
        };
        return CoordinatorServerOptions.FromConfiguration(new ConfigurationBuilder().AddInMemoryCollection(settings).Build());
    }

    [Theory]
    [InlineData(null, "none")]
    [InlineData("", "none")]
    [InlineData("none", "none")]
    [InlineData("encrypted", "encrypted")]
    [InlineData("encrypted-psk", "encrypted-psk")]
    [InlineData("Encrypted-PSK", "encrypted-psk")]
    public void The_Setting_Is_Read_As_A_Word_And_Reads_None_When_It_Is_Absent(string? given, string expected)
    {
        Assert.Equal(expected, Options(given).RequiredProtection);
    }

    [Theory]
    [InlineData("aes")]
    [InlineData("encrypted-psk,none")]
    [InlineData("hunter2")]
    public void Any_Other_Text_Stops_The_Start_Naming_The_Setting_And_The_Three_Words_And_Never_The_Text(string given)
    {
        var failure = Assert.Throws<InvalidOperationException>(() => Options(given));

        Assert.Contains("Coordinator:DataPlane:RequiredProtection must be 'none', 'encrypted' or 'encrypted-psk'.", failure.Message);
        Assert.DoesNotContain(given, failure.Message);
    }

    [Fact]
    public void What_Is_Set_Reaches_Admission_And_Nothing_Set_Requires_Nothing()
    {
        using var unset = new CoordinatorHost();
        using var set = new CoordinatorHost(settings: new Dictionary<string, string> { ["DataPlane:RequiredProtection"] = "encrypted-psk" });

        Assert.Equal(DataPlaneProtections.None, unset.Services.GetRequiredService<AdmissionOptions>().RequiredProtection);
        Assert.Equal(DataPlaneProtections.EncryptedPsk, set.Services.GetRequiredService<AdmissionOptions>().RequiredProtection);
    }
}
