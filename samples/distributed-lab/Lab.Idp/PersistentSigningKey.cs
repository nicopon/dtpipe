using System.Security.Cryptography;
using Microsoft.IdentityModel.Tokens;

namespace DtPipe.Lab.Idp;

/// <summary>
/// The RSA key the IdP signs tokens with: generated the first time the IdP starts and read back
/// afterwards, so a token a client cached before a restart is still valid after it. The file holds a
/// private key: it lives in a state directory git ignores, and is readable by its owner only.
/// </summary>
public static class PersistentSigningKey
{
    public static RsaSecurityKey LoadOrCreate(string path)
    {
        var rsa = RSA.Create(2048);
        if (File.Exists(path))
        {
            rsa.ImportFromPem(File.ReadAllText(path));
        }
        else
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
            File.WriteAllText(temporary, rsa.ExportPkcs8PrivateKeyPem());
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(temporary, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            File.Move(temporary, path);
        }

        // The id derives from the public key, so it is the same after every read of the same file.
        var keyId = Base64UrlEncoder.Encode(SHA256.HashData(rsa.ExportSubjectPublicKeyInfo())[..12]);
        return new RsaSecurityKey(rsa) { KeyId = keyId };
    }
}
