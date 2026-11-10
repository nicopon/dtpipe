using Xunit;

namespace DtPipe.Tests;

/// <summary>
/// Tests that redirect Console.Out / Console.Error must not run in parallel with each
/// other: interleaved SetError/SetOut + restore pairs can leave another test's writes
/// going to the wrong sink. Everything touching console redirection joins this
/// serial collection.
/// </summary>
[CollectionDefinition("console-serial", DisableParallelization = true)]
public sealed class ConsoleSerialCollection;

/// <summary>
/// Process-wide serialization lock for tests that invoke <c>Program.Main</c> or redirect
/// <see cref="Console.Out"/> / <see cref="Console.Error"/>. Prevents asynchronous test
/// interleaving across the thread pool during async awaits.
/// </summary>
public static class ConsoleSerialLock
{
    private static readonly SemaphoreSlim Semaphore = new(1, 1);

    /// <summary>
    /// Executes <paramref name="action"/> while capturing <see cref="Console.Out"/> and
    /// <see cref="Console.Error"/> into a string buffer, restoring the original streams and
    /// releasing the lock upon completion.
    /// </summary>
    public static async Task<(int Exit, string Output)> CaptureAsync(Func<Task<int>> action)
    {
        await Semaphore.WaitAsync();
        var (stdout, stderr) = (Console.Out, Console.Error);
        var captured = new StringWriter();
        Console.SetOut(captured);
        Console.SetError(captured);
        try
        {
            var exit = await action();
            return (exit, captured.ToString());
        }
        finally
        {
            Console.SetOut(stdout);
            Console.SetError(stderr);
            Semaphore.Release();
        }
    }

    /// <summary>
    /// Executes <paramref name="action"/> under the lock, capturing console streams so that
    /// writes do not leak to other tests or standard error.
    /// </summary>
    public static async Task<int> RunAsync(Func<Task<int>> action)
    {
        var (exit, _) = await CaptureAsync(action);
        return exit;
    }
}

