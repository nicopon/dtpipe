using System.Security.Cryptography;
using System.Text;
using DtPipe.Sessions;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Sessions;

/// <summary>
/// The cipher does not promise confidentiality at rest — the key sits on the same disk. It
/// promises two narrower things, and both are tested here: a copy of the project directory is
/// inert without the key, and destroying the key makes every artefact unreadable at once
/// (crypto-shredding), which is what makes a purge reliable rather than best-effort.
/// </summary>
public class CheckpointCipherTests
{
    private static byte[] Key() => RandomNumberGenerator.GetBytes(32);
    private static byte[] Payload(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public void Frames_Round_Trip_In_Order()
    {
        var key = Key();
        var header = CheckpointCipher.CreateHeader();
        using var ms = new MemoryStream();

        for (var i = 0; i < 5; i++)
            CheckpointCipher.WriteFrame(ms, key, header, i, Payload($"frame-{i}"));

        ms.Position = 0;
        var read = new List<string>();
        while (CheckpointCipher.ReadFrame(ms, key, header) is { } frame)
            read.Add(Encoding.UTF8.GetString(frame));

        read.Should().Equal("frame-0", "frame-1", "frame-2", "frame-3", "frame-4");
    }

    [Fact]
    public void Two_Files_Do_Not_Share_A_Nonce_Space()
    {
        var a = CheckpointCipher.CreateHeader();
        var b = CheckpointCipher.CreateHeader();

        a.AsSpan(12, CheckpointCipher.NoncePrefixSize).ToArray()
            .Should().NotEqual(b.AsSpan(12, CheckpointCipher.NoncePrefixSize).ToArray(),
                "the random prefix is what keeps two files under one key from colliding");
    }

    [Fact]
    public void A_Tampered_Ciphertext_Throws_Rather_Than_Returning_Doubtful_Data()
    {
        var key = Key();
        var header = CheckpointCipher.CreateHeader();
        using var ms = new MemoryStream();
        CheckpointCipher.WriteFrame(ms, key, header, 0, Payload("secret payload"));

        var bytes = ms.ToArray();
        bytes[^1] ^= 0xFF;

        var act = () => CheckpointCipher.ReadFrame(new MemoryStream(bytes), key, header);

        act.Should().Throw<CryptographicException>();
    }

    [Fact]
    public void A_Tampered_Header_Throws()
    {
        var key = Key();
        var header = CheckpointCipher.CreateHeader();
        using var ms = new MemoryStream();
        CheckpointCipher.WriteFrame(ms, key, header, 0, Payload("payload"));

        var altered = (byte[])header.Clone();
        altered[12] ^= 0xFF;

        var act = () => CheckpointCipher.ReadFrame(new MemoryStream(ms.ToArray()), key, altered);

        act.Should().Throw<CryptographicException>("the header is authenticated, not merely read");
    }

    [Fact]
    public void The_Wrong_Key_Yields_Nothing_At_All()
    {
        var header = CheckpointCipher.CreateHeader();
        using var ms = new MemoryStream();
        CheckpointCipher.WriteFrame(ms, Key(), header, 0, Payload("payload"));

        var act = () => CheckpointCipher.ReadFrame(new MemoryStream(ms.ToArray()), Key(), header);

        act.Should().Throw<CryptographicException>("no partial plaintext is ever handed back");
    }

    [Fact]
    public void A_Foreign_File_Is_Refused_By_Its_Header()
    {
        var act = () => CheckpointCipher.ValidateHeader(new byte[CheckpointCipher.HeaderSize]);

        act.Should().Throw<CryptographicException>().WithMessage("*checkpoint*");
    }

    [Fact]
    public void A_Truncated_Frame_Throws_Rather_Than_Ending_Quietly()
    {
        var key = Key();
        var header = CheckpointCipher.CreateHeader();
        using var ms = new MemoryStream();
        CheckpointCipher.WriteFrame(ms, key, header, 0, Payload("a reasonably long payload"));

        var truncated = ms.ToArray()[..^5];

        var act = () => CheckpointCipher.ReadFrame(new MemoryStream(truncated), key, header);

        act.Should().Throw<CryptographicException>(
            "a silent short read would look like a clean end of file and lose rows without saying so");
    }

    [Fact]
    public void The_Frame_Counter_Refuses_To_Wrap()
    {
        var act = () => CheckpointCipher.WriteFrame(
            new MemoryStream(), Key(), CheckpointCipher.CreateHeader(), uint.MaxValue, Payload("x"));

        act.Should().Throw<CryptographicException>().WithMessage("*nonce*",
            "continuing past the counter would reuse a nonce, which breaks GCM outright");
    }
}
