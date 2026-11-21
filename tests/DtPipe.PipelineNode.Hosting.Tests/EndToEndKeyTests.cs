using System.Security.Cryptography;
using DtPipe.PipelineNode.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using TransportR.Client;
using Xunit;

namespace DtPipe.PipelineNode.Hosting.Tests;

/// <summary>
/// The node's end-to-end key: off by default, from one source of the node's own configuration, read and checked at start so that
/// a key the node cannot use stops it there, and named by its setting in every refusal, never by its value nor its file.
/// </summary>
public class EndToEndKeyTests : IDisposable
{
    private static readonly byte[] Key = RandomNumberGenerator.GetBytes(32);
    private static readonly string Base64 = Convert.ToBase64String(Key);

    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("end-to-end-key-");

    public void Dispose() => _dir.Delete(recursive: true);

    private static DtPipeNodeOptions Resolve(Action<NodeEncryptionOptions> encryption)
    {
        var services = new ServiceCollection();
        services.AddDtPipeNode(o =>
        {
            o.Name = "crm";
            o.CoordinatorUrl = "http://coordinator:8080";
            o.Oidc.ClientSecret = "secret";
            encryption(o.EndToEndEncryption);
        });
        return services.BuildServiceProvider().GetRequiredService<IOptions<DtPipeNodeOptions>>().Value;
    }

    private static IEnumerable<string> Refusals(Action<NodeEncryptionOptions> encryption) =>
        Assert.Throws<OptionsValidationException>(() => Resolve(encryption)).Failures;

    private static byte[]? KeyOf(DtPipeNodeOptions options) => EndToEndKey.Resolve(options.EndToEndEncryption).Key?.Invoke(Guid.NewGuid());

    [Fact]
    public void Encryption_IsOff_AndTheNodeHoldsNoKey_ByDefault()
    {
        var options = Resolve(_ => { });
        Assert.False(options.EndToEndEncryption.Enabled);
        Assert.Null(EndToEndKey.Resolve(options.EndToEndEncryption).Key);
    }

    [Fact]
    public void TheSettings_AreBoundFromTheNodesSection()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Name"] = "crm",
            ["CoordinatorUrl"] = "http://coordinator:8080",
            ["Oidc:ClientSecret"] = "secret",
            ["EndToEndEncryption:Enabled"] = "true",
            ["EndToEndEncryption:PreSharedKey"] = Base64,
        }).Build();
        var services = new ServiceCollection();
        services.AddDtPipeNode(configuration);
        var options = services.BuildServiceProvider().GetRequiredService<IOptions<DtPipeNodeOptions>>().Value;

        Assert.True(options.EndToEndEncryption.Enabled);
        Assert.Equal(Key, KeyOf(options));
    }

    [Fact]
    public void AKey_IsBase64Of32Bytes_GivenAsAValue_AFile_OrAProvider()
    {
        var file = Path.Combine(_dir.FullName, "domain.key");
        File.WriteAllText(file, "  " + Base64 + "\n");
        Func<Guid, byte[]?> perPeer = _ => Key;

        Assert.Equal(Key, KeyOf(Resolve(e => { e.Enabled = true; e.PreSharedKey = Base64; })));
        Assert.Equal(Key, KeyOf(Resolve(e => { e.Enabled = true; e.PreSharedKey = "@" + file; })));
        Assert.Equal(Key, KeyOf(Resolve(e => { e.Enabled = true; e.PreSharedKeyFile = file; })));
        Assert.Same(perPeer, EndToEndKey.Resolve(Resolve(e => { e.Enabled = true; e.PreSharedKeyProvider = perPeer; }).EndToEndEncryption).Key);
        Assert.Null(EndToEndKey.Resolve(Resolve(e => e.Enabled = true).EndToEndEncryption).Key);
    }

    private static EndToEndSetting SettingOf(Action<NodeEncryptionOptions> encryption) => EndToEndKey.Resolve(Resolve(encryption).EndToEndEncryption);

    /// <summary>The level follows what the client is built with: the same resolution gives both, so the announcement cannot say more.</summary>
    [Fact]
    public void TheProtection_IsWhatTheClientIsBuiltWith_AndOnlyAConfiguredKeyHasAnId()
    {
        var off = SettingOf(_ => { });
        var sealedWithNoKey = SettingOf(e => e.Enabled = true);
        var sealedWithTheKey = SettingOf(e => { e.Enabled = true; e.PreSharedKey = Base64; });
        var sealedPerPeer = SettingOf(e => { e.Enabled = true; e.PreSharedKeyProvider = _ => Key; });

        Assert.False(off.Enabled);
        Assert.Null(off.Key);
        Assert.Equal("none", off.Protection);
        Assert.Null(off.KeyId);
        Assert.True(sealedWithNoKey.Enabled);
        Assert.Null(sealedWithNoKey.Key);
        Assert.Equal("encrypted", sealedWithNoKey.Protection);
        Assert.Null(sealedWithNoKey.KeyId);
        Assert.True(sealedWithTheKey.Enabled);
        Assert.NotNull(sealedWithTheKey.Key);
        Assert.Equal("encrypted-psk", sealedWithTheKey.Protection);
        Assert.NotNull(sealedWithTheKey.KeyId);
        Assert.True(sealedPerPeer.Enabled);
        Assert.NotNull(sealedPerPeer.Key);
        Assert.Equal("encrypted-psk", sealedPerPeer.Protection);
        Assert.Null(sealedPerPeer.KeyId);
    }

    /// <summary>
    /// What a client reports of a transfer reads through the table the announced level reads through, so one level has one word. A value
    /// the table does not know (a later TransportR's) has no word and no entry in an exit, and raises nothing.
    /// </summary>
    [Fact]
    public void TheProtectionAClientReports_HasTheWordOfTheAnnouncedLevel_AndAValueTheTableDoesNotKnowHasNoWord()
    {
        Assert.Equal(SettingOf(_ => { }).Protection, EndToEndKey.WordOf(TransferProtection.None));
        Assert.Equal(SettingOf(e => e.Enabled = true).Protection, EndToEndKey.WordOf(TransferProtection.Encrypted));
        Assert.Equal(SettingOf(e => { e.Enabled = true; e.PreSharedKey = Base64; }).Protection, EndToEndKey.WordOf(TransferProtection.EncryptedWithPreSharedKey));
        var future = (TransferProtection)99;
        Assert.Null(EndToEndKey.WordOf(future));

        var words = EndToEndKey.WordsOf(new Dictionary<string, TransferProtection> { ["in"] = TransferProtection.Encrypted, ["out"] = future });
        var only = Assert.Single(words!);
        Assert.Equal(("in", "encrypted"), (only.Key, only.Value));
        Assert.Null(EndToEndKey.WordsOf(new Dictionary<string, TransferProtection> { ["out"] = future }));
        Assert.Null(EndToEndKey.WordsOf(new Dictionary<string, TransferProtection>()));
    }

    [Fact]
    public void TheKeyId_IsAFingerprintOfTheKey_TheSameForTheSameKey_AndNeverTheKey()
    {
        var file = Path.Combine(_dir.FullName, "domain.key");
        File.WriteAllText(file, Base64 + "\n");
        var other = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));

        var id = SettingOf(e => { e.Enabled = true; e.PreSharedKey = Base64; }).KeyId!;
        var sameFromAFile = SettingOf(e => { e.Enabled = true; e.PreSharedKeyFile = file; }).KeyId;
        var ofAnotherKey = SettingOf(e => { e.Enabled = true; e.PreSharedKey = other; }).KeyId;

        Assert.Matches("^[0-9a-f]{16}$", id);
        Assert.Equal(id, sameFromAFile);
        Assert.NotEqual(id, ofAnotherKey);
        // The derivation every node applies: the first 8 bytes of HMAC-SHA256, keyed with the key, over this label.
        Assert.Equal(Convert.ToHexString(HMACSHA256.HashData(Key, "dtpipe end-to-end key id"u8.ToArray())[..8]).ToLowerInvariant(), id);
        Assert.DoesNotContain(Base64, id);
        Assert.DoesNotContain(Convert.ToHexString(Key).ToLowerInvariant(), id);
        Assert.DoesNotContain(Convert.ToHexString(Key)[..16].ToLowerInvariant(), id);
    }

    [Fact]
    public void AKeyWithoutEncryption_OrFromTwoSources_StopsTheStart()
    {
        Assert.Contains("DtPipeNode: EndToEndEncryption:PreSharedKey is set while EndToEndEncryption:Enabled is off.",
            Refusals(e => e.PreSharedKey = Base64));
        Assert.Contains("DtPipeNode: EndToEndEncryption:PreSharedKey, EndToEndEncryption:PreSharedKeyFile and a key provider are exclusive: set one.",
            Refusals(e => { e.Enabled = true; e.PreSharedKey = Base64; e.PreSharedKeyProvider = _ => Key; }));
    }

    [Fact]
    public void AKeyTheNodeCannotUse_StopsTheStart_NamingTheSetting_NeverTheKeyNorItsFile()
    {
        var shortKey = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));
        Assert.Equal("DtPipeNode: EndToEndEncryption:PreSharedKey is not base64 of 32 bytes.",
            Assert.Single(Refusals(e => { e.Enabled = true; e.PreSharedKey = shortKey; })));
        Assert.Equal("DtPipeNode: EndToEndEncryption:PreSharedKey is not base64 of 32 bytes.",
            Assert.Single(Refusals(e => { e.Enabled = true; e.PreSharedKey = "not a key"; })));

        // A key pasted after @ by mistake names a file that does not exist: the refusal shows neither.
        var pasted = Assert.Single(Refusals(e => { e.Enabled = true; e.PreSharedKey = "@" + Base64; }));
        Assert.Contains("EndToEndEncryption:PreSharedKey names a file (@…) that cannot be read", pasted);
        Assert.DoesNotContain(Base64, pasted);

        var file = Path.Combine(_dir.FullName, "short.key");
        File.WriteAllText(file, shortKey);
        var fromFile = Assert.Single(Refusals(e => { e.Enabled = true; e.PreSharedKeyFile = file; }));
        Assert.Equal("DtPipeNode: EndToEndEncryption:PreSharedKeyFile is not base64 of 32 bytes.", fromFile);
        Assert.DoesNotContain(file, fromFile);
        Assert.DoesNotContain(shortKey, fromFile);
    }
}
