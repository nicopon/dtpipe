namespace DtPipe.Cli.Studio;

/// <summary>A request <c>dtpipe studio</c> refuses, with the status the page is told.</summary>
public sealed class StudioRefusal(int status, string message) : Exception(message)
{
    public int Status { get; } = status;
}

/// <summary>
/// The folder <c>dtpipe studio</c> was opened on: the only files the page may read, its YAML jobs.
/// A path is confined to the folder as written and as stored: no segment starting with a dot (so no
/// <c>..</c> and nothing hidden), and no symbolic link below the folder, which could point anywhere.
/// </summary>
public sealed class StudioFolder
{
    private const int MaxJobs = 1000;
    private const long MaxJobBytes = 1_000_000;

    private static readonly EnumerationOptions Walk = new()
    {
        RecurseSubdirectories = true,
        MaxRecursionDepth = 8,
        IgnoreInaccessible = true,
        AttributesToSkip = FileAttributes.Hidden | FileAttributes.System | FileAttributes.ReparsePoint,
    };

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

    public string Read(string relative)
    {
        var path = Resolve(relative);
        if (new FileInfo(path).Length > MaxJobBytes)
            throw new StudioRefusal(413, $"'{relative}' is larger than {MaxJobBytes:N0} bytes.");
        return File.ReadAllText(path);
    }

    /// <summary>The full path of a job of the folder, or a refusal that names why.</summary>
    public string Resolve(string relative)
    {
        if (string.IsNullOrWhiteSpace(relative) || Path.IsPathRooted(relative))
            throw new StudioRefusal(400, "A job is named by its path relative to the folder.");
        if (relative.Split('/', '\\').Any(s => s.Length == 0 || s.StartsWith('.')))
            throw new StudioRefusal(400, $"'{relative}' leaves the folder or names a hidden file.");
        if (!IsJob(relative))
            throw new StudioRefusal(400, $"'{relative}' is not a .yaml or .yml file.");

        var full = Path.GetFullPath(Path.Combine(Root, relative));
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (!full.StartsWith(Root + Path.DirectorySeparatorChar, comparison))
            throw new StudioRefusal(400, $"'{relative}' leaves the folder.");
        if (!File.Exists(full))
            throw new StudioRefusal(404, $"No job at '{relative}'.");

        for (var p = full; p.Length > Root.Length; p = Path.GetDirectoryName(p)!)
            if ((File.GetAttributes(p) & FileAttributes.ReparsePoint) != 0)
                throw new StudioRefusal(400, $"'{relative}' goes through a link, which could lead out of the folder.");
        return full;
    }

    private static bool IsJob(string path) =>
        Path.GetExtension(path) is var ext
        && (ext.Equals(".yaml", StringComparison.OrdinalIgnoreCase) || ext.Equals(".yml", StringComparison.OrdinalIgnoreCase));
}
