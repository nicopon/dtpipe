using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Sessions;

namespace DtPipe.Cli.Studio;

/// <summary>A request <c>dtpipe studio</c> refuses, with the status the page is told.</summary>
public sealed class StudioRefusal(int status, string message) : Exception(message)
{
    public int Status { get; } = status;
}

/// <summary>A job as the page opened it: its text, and the hash a save must still find on disk.</summary>
public sealed record StudioJob(string Text, string Hash);

/// <summary>
/// The folder <c>dtpipe studio</c> was opened on: the only files the page may read or write, its YAML
/// jobs. A path is confined to the folder as written and as stored: no segment starting with a dot
/// (so no <c>..</c> and nothing hidden), and no symbolic link below the folder, which could point
/// anywhere. A write replaces a file whole, never creates a folder, and never follows a link.
/// </summary>
public sealed class StudioFolder
{
    private const int MaxJobs = 1000;
    public const long MaxJobBytes = 1_000_000;

    private static readonly UTF8Encoding Utf8 = new(encoderShouldEmitUTF8Identifier: false);

    private static readonly EnumerationOptions Walk = new()
    {
        RecurseSubdirectories = true,
        MaxRecursionDepth = 8,
        IgnoreInaccessible = true,
        AttributesToSkip = FileAttributes.Hidden | FileAttributes.System | FileAttributes.ReparsePoint,
    };

    private static readonly StringComparison PathComparison =
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    public string Root { get; }

    public StudioFolder(string path)
    {
        Root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        if (!Directory.Exists(Root)) throw new DirectoryNotFoundException($"No folder at {Root}.");
    }

    /// <summary>The jobs under the folder, as relative paths with '/' separators, in order.</summary>
    public IReadOnlyList<string> Jobs() =>
        Directory.EnumerateFiles(Root, "*", Walk)
            .Where(IsJob)
            .Select(f => Path.GetRelativePath(Root, f).Replace(Path.DirectorySeparatorChar, '/'))
            .Where(r => !r.Split('/').Any(s => s.StartsWith('.')))
            .Order(StringComparer.Ordinal)
            .Take(MaxJobs)
            .ToList();

    public string Read(string relative) => ReadJob(relative).Text;

    public StudioJob ReadJob(string relative)
    {
        var path = Resolve(relative);
        if (new FileInfo(path).Length > MaxJobBytes)
            throw new StudioRefusal(413, $"'{relative}' is larger than {MaxJobBytes:N0} bytes.");
        var bytes = File.ReadAllBytes(path);
        return new StudioJob(new StreamReader(new MemoryStream(bytes), Utf8).ReadToEnd(), Hash(bytes));
    }

    /// <summary>
    /// Writes a job whole. <paramref name="expectedHash"/> is the hash the page read: null creates the
    /// file and refuses one already there; otherwise the file must still hold what the page opened.
    /// Returns the new hash.
    /// </summary>
    public string WriteJob(string relative, string text, string? expectedHash)
    {
        var bytes = Utf8.GetBytes(text.Replace("\r\n", "\n"));
        if (bytes.Length > MaxJobBytes)
            throw new StudioRefusal(413, $"The job would be larger than {MaxJobBytes:N0} bytes.");
        var path = ResolveForWrite(relative);
        lock (this)
        {
            if (expectedHash is null)
            {
                if (File.Exists(path)) throw new StudioRefusal(409, $"'{relative}' already exists: open it to change it.");
            }
            else if (!File.Exists(path))
                throw new StudioRefusal(409, $"'{relative}' was removed since it was opened.");
            else if (!string.Equals(Hash(File.ReadAllBytes(path)), expectedHash, StringComparison.Ordinal))
                throw new StudioRefusal(409, $"'{relative}' changed on disk since it was opened: open it again, then redo the change.");
            WriteAtomic(path, bytes);
        }
        return Hash(bytes);
    }

    /// <summary>The full path of a job of the folder, or a refusal that names why.</summary>
    public string Resolve(string relative)
    {
        var full = Confine(relative);
        if (!File.Exists(full))
            throw new StudioRefusal(404, $"No job at '{relative}'.");
        RefuseLinks(relative, full);
        return full;
    }

    /// <summary>
    /// The full path a job may be written at: its folder already exists, nothing on the way is a
    /// link, and the target, if there, is a plain file.
    /// </summary>
    public string ResolveForWrite(string relative)
    {
        var full = Confine(relative);
        var parent = Path.GetDirectoryName(full)!;
        if (!Directory.Exists(parent))
            throw new StudioRefusal(404, $"No folder for '{relative}': the studio creates none.");
        if (Directory.Exists(full))
            throw new StudioRefusal(400, $"'{relative}' is a folder.");
        RefuseLinks(relative, File.Exists(full) ? full : parent);
        return full;
    }

    private string Confine(string relative)
    {
        if (string.IsNullOrWhiteSpace(relative) || Path.IsPathRooted(relative))
            throw new StudioRefusal(400, "A job is named by its path relative to the folder.");
        if (relative.Split('/', '\\').Any(s => s.Length == 0 || s.StartsWith('.')))
            throw new StudioRefusal(400, $"'{relative}' leaves the folder or names a hidden file.");
        if (!IsJob(relative))
            throw new StudioRefusal(400, $"'{relative}' is not a .yaml or .yml file.");

        var full = Path.GetFullPath(Path.Combine(Root, relative));
        if (!full.StartsWith(Root + Path.DirectorySeparatorChar, PathComparison))
            throw new StudioRefusal(400, $"'{relative}' leaves the folder.");
        return full;
    }

    private void RefuseLinks(string relative, string from)
    {
        for (var p = from; p.Length > Root.Length; p = Path.GetDirectoryName(p)!)
            if ((File.GetAttributes(p) & FileAttributes.ReparsePoint) != 0)
                throw new StudioRefusal(400, $"'{relative}' goes through a link, which could lead out of the folder.");
    }

    /// <summary>Replaces a file whole: written beside it under a hidden name, then moved over it.</summary>
    internal static void WriteAtomic(string path, byte[] bytes)
    {
        var temp = Path.Combine(Path.GetDirectoryName(path)!, $".{Path.GetFileName(path)}.{Guid.NewGuid():N}.tmp");
        try
        {
            File.WriteAllBytes(temp, bytes);
            File.Move(temp, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temp)) File.Delete(temp);
        }
    }

    internal static string Hash(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));

    private static bool IsJob(string path) =>
        Path.GetExtension(path) is var ext
        && (ext.Equals(".yaml", StringComparison.OrdinalIgnoreCase) || ext.Equals(".yml", StringComparison.OrdinalIgnoreCase));
}

/// <summary>A job published from this studio: to which coordinator, under which id, at which commit of its job.</summary>
public sealed record StudioPublication(string Coordinator, string Id, string? Commit);

/// <summary>
/// What the studio keeps for a job beside the folder's files, never in them and never in git: where
/// its cards were drawn (<c>&lt;job&gt;-layout.json</c>), what its variables are launched with
/// (<c>&lt;job&gt;-launch.json</c>: a keyring alias or a typed text, never a keyring value) and where it
/// was published last (<c>&lt;job&gt;-publish.json</c>), under
/// <c>.dtpipe/studio/</c>, whose <c>.dtpipe</c> ignores itself. No directory on the way may be a link:
/// a clone could carry a <c>.dtpipe</c> that points anywhere. A job is a path the folder confined.
/// </summary>
public sealed class StudioLocalFiles(StudioFolder folder)
{
    private const long MaxBytes = 1_000_000;

    private string DtpipeDirectory => Path.Combine(folder.Root, SessionResolver.DirectoryName);

    public JsonObject? ReadLayout(string job) => Read(job, "layout") as JsonObject;

    public void WriteLayout(string job, JsonObject layout) => Write(job, "layout", layout);

    /// <summary>Refuses, before anything is written, a layout the studio could only keep through a link.</summary>
    public void CheckLayout(string job) => RefuseLinks(PathOf(job, "layout"));

    private static readonly JsonSerializerOptions LaunchJson = new(JsonSerializerOptions.Web)
    {
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true,
    };

    public Dictionary<string, StudioOverride> ReadLaunch(string job)
    {
        try { return Read(job, "launch")?.Deserialize<Dictionary<string, StudioOverride>>(LaunchJson) ?? []; }
        catch (JsonException) { return []; }
    }

    public void WriteLaunch(string job, IReadOnlyDictionary<string, StudioOverride> overrides) =>
        Write(job, "launch", JsonSerializer.SerializeToNode(overrides, LaunchJson)!);

    /// <summary>Where a job was published last from here, for the next publication to name the version it replaces.</summary>
    public StudioPublication? ReadPublication(string job)
    {
        try { return Read(job, "publish")?.Deserialize<StudioPublication>(LaunchJson); }
        catch (JsonException) { return null; }
    }

    public void WritePublication(string job, StudioPublication publication) =>
        Write(job, "publish", JsonSerializer.SerializeToNode(publication, LaunchJson)!);

    private string SamplesDirectory => Path.Combine(DtpipeDirectory, "studio", "samples");

    /// <summary>
    /// A new throwaway file under <c>.dtpipe/studio/samples/</c>, created readable by its owner only
    /// and holding <paramref name="bytes"/>; with none, only its path, for a run to write. The caller
    /// deletes it.
    /// </summary>
    public string SampleFile(byte[]? bytes)
    {
        RefuseLinks(SamplesDirectory);
        SessionStore.EnsureIgnored(DtpipeDirectory);
        Directory.CreateDirectory(SamplesDirectory);
        RefuseLinks(SamplesDirectory);
        var path = Path.Combine(SamplesDirectory, Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16)) + ".arrows");
        if (bytes is null) return path;
        var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        using var stream = new FileStream(path, options);
        stream.Write(bytes);
        return path;
    }

    /// <summary>Removes what a studio that stopped short left under <c>.dtpipe/studio/samples/</c>; never through a link.</summary>
    public void PurgeSamples()
    {
        try
        {
            RefuseLinks(SamplesDirectory);
            if (!Directory.Exists(SamplesDirectory)) return;
            foreach (var file in Directory.EnumerateFiles(SamplesDirectory))
                if ((File.GetAttributes(file) & FileAttributes.ReparsePoint) == 0) File.Delete(file);
        }
        catch (Exception ex) when (ex is StudioRefusal or IOException or UnauthorizedAccessException) { }
    }

    private JsonNode? Read(string job, string kind)
    {
        var path = PathOf(job, kind);
        RefuseLinks(path);
        if (!File.Exists(path) || new FileInfo(path).Length > MaxBytes) return null;
        try { return JsonNode.Parse(File.ReadAllText(path)); }
        catch (JsonException) { return null; }
    }

    private void Write(string job, string kind, JsonNode value)
    {
        var path = PathOf(job, kind);
        RefuseLinks(path);
        SessionStore.EnsureIgnored(DtpipeDirectory);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        StudioFolder.WriteAtomic(path, Encoding.UTF8.GetBytes(value.ToJsonString(new JsonSerializerOptions { WriteIndented = true })));
    }

    private string PathOf(string job, string kind) =>
        Path.GetFullPath(Path.Combine(DtpipeDirectory, "studio", job.Replace('\\', '/') + $"-{kind}.json"));

    /// <summary>Every existing segment from <c>.dtpipe</c> down to the file is a plain directory or file.</summary>
    private void RefuseLinks(string path)
    {
        for (var p = path; p.Length > folder.Root.Length; p = Path.GetDirectoryName(p)!)
        {
            if (!File.Exists(p) && !Directory.Exists(p)) continue;
            if ((File.GetAttributes(p) & FileAttributes.ReparsePoint) != 0)
                throw new StudioRefusal(400, $"{Path.GetRelativePath(folder.Root, p)} is a link: the studio keeps nothing through one.");
        }
    }
}
