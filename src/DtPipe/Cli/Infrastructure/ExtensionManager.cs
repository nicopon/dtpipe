using System.Reflection;
using System.Runtime.InteropServices;

namespace DtPipe.Cli.Infrastructure;

public static class ExtensionManager
{
    public static async Task<string?> GetExtensionPathAsync(string extensionName)
    {
        var platform = GetDuckDbPlatform();
        if (platform == null) return null;

        // Resource naming usually maps subfolders to dots
        var resourceName = $"DtPipe.Resources.Extensions.{platform}.{extensionName}.duckdb_extension";
        var assembly = Assembly.GetExecutingAssembly();

        using var stream = assembly.GetManifestResourceStream(resourceName);
        if (stream == null)
        {
            // Try listing resource names for debugging if not found
            var names = assembly.GetManifestResourceNames();
            Console.Error.WriteLine($"Resource not found: {resourceName}");
            // Console.Error.WriteLine("Available resources: " + string.Join(", ", names));
            return null;
        }

        var tempDir = Path.Combine(Path.GetTempPath(), "dtpipe", "extensions", platform);
        Directory.CreateDirectory(tempDir);

        var extensionPath = Path.Combine(tempDir, $"{extensionName}.duckdb_extension");

        // Write to temp path
        using (var fileStream = File.Create(extensionPath))
        {
            await stream.CopyToAsync(fileStream);
        }

        return extensionPath;
    }

    private static string? GetDuckDbPlatform()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ? "osx_arm64" : "osx_amd64";
        }
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return "windows_amd64"; // DuckDB uses windows_amd64 or win_amd64 depending on version, let's check
        }
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return "linux_amd64";
        }
        return null;
    }
}
