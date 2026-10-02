using System.Diagnostics;

namespace DtPipe.PipelineNode.Tests;

/// <summary>Runs the real <c>dtpipe</c> binary to completion, for fixture setup — never for a guard's own assertion.</summary>
internal static class DtPipeCli
{
    public static void Run(params string[] arguments)
    {
        var psi = new ProcessStartInfo(DtPipeExecutableLocator.Path)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var arg in arguments) psi.ArgumentList.Add(arg);

        using var process = Process.Start(psi)!;
        string stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"dtpipe {string.Join(' ', arguments)} exited {process.ExitCode}: {stderr}");
    }
}
