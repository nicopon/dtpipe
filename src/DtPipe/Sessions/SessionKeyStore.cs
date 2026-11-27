using System.Security.Cryptography;

namespace DtPipe.Sessions;

/// <summary>
/// Holds one AES key per session, in user state — <b>deliberately not beside the ciphertext</b>.
///
/// What that separation buys, and it is not confidentiality at rest:
///
/// <list type="bullet">
/// <item><b>An inert copy.</b> The project directory can be rsynced, backed up, synced to a
/// cloud folder, archived or `git add -A`-ed; the ciphertext travels without the key and says
/// nothing.</item>
/// <item><b>A reliable purge.</b> Deleting the key makes every artefact unreadable at once, even
/// if removing the files then fails halfway. Crypto-shredding, not tidying.</item>
/// </list>
///
/// What it does not buy: protection from someone with access to the account — they have the key
/// too. The disk is usually already encrypted (FileVault, BitLocker) and that is the layer doing
/// that job.
///
/// The OS keyring is deliberately NOT used. It is for what a user chose to keep
/// (<c>dtpipe secret</c>), not for what the tool makes and destroys: an unsigned binary triggers
/// a keychain prompt per access on macOS, and making a checkpoint read depend on an unlocked
/// keyring breaks CI outright.
/// </summary>
public static class SessionKeyStore
{
    private const int KeySizeBytes = 32; // AES-256
    private const string LockSuffix = ".lock";

    public static string KeyPath(string sessionName)
        => Path.Combine(UserStatePaths.KeysDirectory(), $"{SessionResolver.Sanitize(sessionName)}.key");

    /// <summary>
    /// Returns this session's key, creating it on first use. Two callers meeting a session with
    /// no key (two branches of one run, two processes) get the same one: the file is created
    /// exclusively, and a caller that lost the creation waits for the winner's bytes.
    /// </summary>
    /// <exception cref="IOException">
    /// If the key directory cannot be written to. This fails the run on purpose: falling back to
    /// writing artefacts in the clear would silently void both properties above, for the whole
    /// store and retroactively — every other session in it included.
    /// </exception>
    public static byte[] GetOrCreateKey(string sessionName)
    {
        var path = KeyPath(sessionName);
        var dir = Path.GetDirectoryName(path)!;
        try
        {
            if (OperatingSystem.IsWindows()) Directory.CreateDirectory(dir);
            else Directory.CreateDirectory(dir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            // Bounded: a creator that died after creating the file leaves it short for good, and
            // only that is replaced under an exclusive lock file, since a key can only produce
            // unreadable artefacts if truncated.
            if (TryAcquireKey(path, attempts: 100) is { } key) return key;
            return RecoverTruncatedKey(path)
                ?? throw new IOException($"The key file '{path}' is held without holding a key.");
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
        {
            throw new IOException(
                $"dtpipe could not write the session key to '{dir}'. Materialisation is refused rather " +
                $"than writing artefacts unencrypted — the guarantee is a property of the whole store, " +
                $"so one cleartext session would void it for all of them. Original error: {ex.Message}", ex);
        }
    }

    private static byte[]? TryAcquireKey(string path, int attempts)
    {
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (TryReadKeyWhileCreating(path) is { } existing) return existing;
            if (TryCreate(path) is { } created) return created;
            Thread.Sleep(10);
        }
        return null;
    }

    /// <summary>
    /// Replaces a key file left short for good, under an exclusive lock file. The lock file is never deleted: a waiter that opened it
    /// and a newcomer creating a fresh one would both hold an exclusive lock. Under the lock a file is removed only once it has been
    /// read short twice, a moment apart: a creator holds its file exclusively while it writes, so a file busy or short only once
    /// may be one being written.
    /// </summary>
    private static byte[]? RecoverTruncatedKey(string path)
    {
        var lockPath = path + LockSuffix;
        var lockOptions = new FileStreamOptions
        {
            Mode = FileMode.OpenOrCreate,
            Access = FileAccess.ReadWrite,
            Share = FileShare.None,
        };
        if (!OperatingSystem.IsWindows()) lockOptions.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;

        FileStream? lockStream = null;
        for (var attempt = 0; attempt < 100; attempt++)
        {
            try
            {
                lockStream = new FileStream(lockPath, lockOptions);
                break;
            }
            catch (IOException)
            {
                if (TryReadKeyWhileCreating(path) is { } ready) return ready;
                Thread.Sleep(10);
            }
        }
        if (lockStream is null) return TryReadKeyWhileCreating(path);

        using (lockStream)
        {
            var seenShort = false;
            for (var attempt = 0; attempt < 100; attempt++)
            {
                switch (ProbeKeyFile(path, out var key))
                {
                    case KeyFileState.Ready:
                        return key;
                    case KeyFileState.Absent:
                        if (TryCreate(path) is { } created) return created;
                        seenShort = false;
                        break;
                    case KeyFileState.WrongLength when seenShort:
                        try { File.Delete(path); } catch (IOException) { }
                        seenShort = false;
                        continue;
                    case KeyFileState.WrongLength:
                        seenShort = true;
                        break;
                    default:
                        seenShort = false;
                        break;
                }
                Thread.Sleep(10);
            }
            return null;
        }
    }

    /// <summary>Creates the file and writes a new key into it, or returns null if the file exists.</summary>
    private static byte[]? TryCreate(string path)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
        };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite; // NTFS inherits the user profile's ACL.
        FileStream file;
        try { file = new FileStream(path, options); }
        catch (IOException) when (File.Exists(path)) { return null; }
        catch (IOException)
        {
            // Refused, and nothing is there now: a recoverer removed the file in between. Once more, so that a refusal that lasts
            // (a full or read-only disk) is reported as itself rather than read as a file another caller holds.
            try { file = new FileStream(path, options); }
            catch (IOException) when (File.Exists(path)) { return null; }
        }
        try
        {
            using (file)
            {
                var key = RandomNumberGenerator.GetBytes(KeySizeBytes);
                file.Write(key);
                return key;
            }
        }
        catch
        {
            try { File.Delete(path); } catch (IOException) { }
            throw;
        }
    }

    /// <summary>Returns the key if it exists, or null. Never creates one.</summary>
    public static byte[]? TryGetKey(string sessionName) => TryReadKey(KeyPath(sessionName));

    private static byte[]? TryReadKey(string path) => ReadKeyFile(path, out var key) == KeyFileState.Ready ? key : null;

    private enum KeyFileState { Ready, Absent, WrongLength, Busy }

    private static KeyFileState ReadKeyFile(string path, out byte[]? key)
    {
        key = null;
        FileStream file;
        try { file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read); }
        catch (FileNotFoundException) { return KeyFileState.Absent; }
        catch (DirectoryNotFoundException) { return KeyFileState.Absent; }

        using (file)
        {
            var buffer = new byte[KeySizeBytes + 1];
            var total = 0;
            while (total < buffer.Length)
            {
                var read = file.Read(buffer, total, buffer.Length - total);
                if (read == 0) break;
                total += read;
            }
            if (total != KeySizeBytes) return KeyFileState.WrongLength;
            key = buffer[..KeySizeBytes];
            return KeyFileState.Ready;
        }
    }

    // A creator holding FileShare.None causes a sharing-violation IOException until it closes the file;
    // that is "busy", so the retry loops wait instead of aborting the race or taking the file for a short one.
    private static KeyFileState ProbeKeyFile(string path, out byte[]? key)
    {
        try { return ReadKeyFile(path, out key); }
        catch (IOException) { key = null; return KeyFileState.Busy; }
    }

    private static byte[]? TryReadKeyWhileCreating(string path) => ProbeKeyFile(path, out var key) == KeyFileState.Ready ? key : null;

    /// <summary>Destroys a session's key. This is what makes its artefacts unreadable.</summary>
    public static void DeleteKey(string sessionName)
    {
        var path = KeyPath(sessionName);
        if (File.Exists(path)) File.Delete(path);
        try { File.Delete(path + LockSuffix); } catch (IOException) { }
    }
}
