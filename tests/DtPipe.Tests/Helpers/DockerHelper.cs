using System.Diagnostics;

namespace DtPipe.Tests.Helpers;

public static class DockerHelper
{
    private static bool? _isAvailable;

    public static bool IsAvailable()
    {
        if (_isAvailable.HasValue) return _isAvailable.Value;

        try
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "docker",
                    Arguments = "info",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };
            
            process.Start();
            process.WaitForExit(3000); // Allow bit more time for info
            
            _isAvailable = process.ExitCode == 0;
        }
        catch
        {
            _isAvailable = false;
        }

        return _isAvailable.Value;
    }
}
