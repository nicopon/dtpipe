using System.Security.Cryptography;
using Microsoft.IdentityModel.Tokens;

namespace DtPipe.Lab.Coordinator;

/// <summary>
/// The RSA key the embedded IDP signs tokens with: generated the first time the coordinator starts and
/// read back from the state directory afterwards, so a token a node host cached before a coordinator
/// restart is still valid after it. The file holds a private key: it lives under the state directory,
/// which git ignores, and is readable by its owner only.
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
