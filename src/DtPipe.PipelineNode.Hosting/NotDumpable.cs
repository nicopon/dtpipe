using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// Makes a runner's process not dumpable on Linux before it starts a child: its <c>/proc</c> entries, its environment (which holds
/// its key) and its memory among them, become root's, so a step it runs under the same user reads neither. A child is dumpable
/// again from its own start, so the runner still watches and ends it. Anywhere else this does nothing.
/// </summary>
internal static class NotDumpable
{
    private const int PrSetDumpable = 4;

    [DllImport("libc", SetLastError = true)]
    private static extern int prctl(int option, nuint arg2, nuint arg3, nuint arg4, nuint arg5);

    /// <summary>Applies it; a kernel that refuses is logged, and the runner still starts.</summary>
    public static void Apply(ILogger logger)
    {
        if (!OperatingSystem.IsLinux()) return;
        if (prctl(PrSetDumpable, 0, 0, 0, 0) != 0)
            logger.LogWarning("The runner could not make itself not dumpable (errno {Errno}): a step under its user can read its environment",
                Marshal.GetLastPInvokeError());
    }
}
