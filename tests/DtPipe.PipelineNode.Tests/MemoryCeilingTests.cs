using System.Runtime.InteropServices;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

public class MemoryCeilingTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan SettleDuration = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan PauseDuration = TimeSpan.FromSeconds(5);

    // macOS/Darwin values (SDK sys/signal.h) — do not port as-is to Linux.
    private const int SIGSTOP = 17;
    private const int SIGCONT = 19;

    [DllImport("libc", SetLastError = true)]
    private static extern int kill(int pid, int sig);

    /// <summary>
    /// Stops the consumer's child right after it starts, before either side is wired, so its stdin
    /// pipe never drains once bytes start arriving — the block that follows has to propagate back
    /// through TransportR's receive capacity to the sender rather than being absorbed in this
    /// process's own heap. The payload is <c>--fake "noise:random.guid"</c> ahead of the cut so it
    /// actually crosses the wire (LZ4 does not shrink random bytes); a plain generated index column
    /// would compress away and under-report what the sender is holding.
    ///
    /// The claim under test is two-part, not a sized ceiling: the producer's child is still alive
    /// (the source has not run dry) <em>and</em> heap growth stays flat once the initial fill settles
    /// — together, that means the sender is blocked rather than still racing ahead. Neither half
    /// proves it alone: a flat heap with an exited child just means the source finished; a live child
    /// with unbounded growth just means the fill hasn't caught up yet. <c>GC.GetTotalMemory</c> reads
    /// the whole host process — both nodes' clients and the in-process hub — so no absolute number is
    /// asserted here; see the plan doc for the measured magnitude and how it was obtained.
    /// </summary>
    [Fact]
    public async Task PausingTheConsumer_BoundsSenderMemory()
    {
        Assert.SkipUnless(OperatingSystem.IsMacOS(),
            "SIGSTOP/SIGCONT values below are Darwin's; this test needs its own Linux constants first.");

        using var fixture = SplitFixture.Create(rowCount: 2_000_000, cutAt: 1, extraArgs: ["--fake", "noise:random.guid"]);
        await using var host = await NodeTestHost.StartAsync();

        await using var producer = await PipelineNode.StartAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = fixture.ProducerJobPath,
            Edges = [new EdgeBinding("main", EdgeDirection.Outbound)],
        });
        await using var consumer = await PipelineNode.StartAsync(new PipelineNodeOptions
        {
            HubUrl = host.Url,
            DtPipeExecutable = DtPipeExecutableLocator.Path,
            FragmentJobPath = fixture.ConsumerJobPath,
            Edges = [new EdgeBinding("main", EdgeDirection.Inbound)],
        });

        Signal(consumer.ChildProcessId, SIGSTOP);
        try
        {
            var transferId = await host.TransferInitiator.InitTransferAsync(
                producer.ClientId, consumer.ClientId, batchSize: 8, timeoutMs: 20_000).WaitAsync(Bound);

            await Task.WhenAll(
                producer.WireAsync("main", transferId).WaitAsync(Bound),
                consumer.WireAsync("main", transferId).WaitAsync(Bound));

            await Task.Delay(SettleDuration);
            var settled = GC.GetTotalMemory(forceFullCollection: true);

            await Task.Delay(PauseDuration);

            Assert.False(producer.ChildHasExited,
                "producer's child finished sending despite the consumer being stopped — no backpressure reached it");

            var afterPause = GC.GetTotalMemory(forceFullCollection: true);
            const long growthMarginBytes = 8 * 1024 * 1024; // observed drift while plateaued is under 1 MB
            Assert.True(afterPause - settled < growthMarginBytes,
                $"node memory grew by {(afterPause - settled) / 1024}KB after settling, " +
                $"past the {growthMarginBytes / 1024}KB margin — backlog kept growing while the consumer was stopped");
        }
        finally
        {
            Signal(consumer.ChildProcessId, SIGCONT);
        }

        var exitCodes = await Task.WhenAll(
            producer.RunToCompletionAsync().WaitAsync(Bound),
            consumer.RunToCompletionAsync().WaitAsync(Bound));

        Assert.Equal(0, exitCodes[0]);
        Assert.Equal(0, exitCodes[1]);
        Assert.Equal(fixture.RowCount, producer.RowCounts["main"]);
        Assert.Equal(fixture.RowCount, consumer.RowCounts["main"]);
    }

    // Sends the signal via libc kill(2) directly rather than through System.Diagnostics.Process
    // ("kill" as a child process). Spawning a tracked Process here deadlocks: with the consumer's
    // child stopped, disposing that unrelated one-shot Process blocks in
    // ProcessWaitState.ReleaseRef() waiting on a lock the runtime's own SIGCHLD reaper
    // (Process.OnSigChild -> ProcessWaitState.CheckChildren) was holding while it burned CPU —
    // observed directly via `dotnet-stack report` on a hung run. A maintainer who "simplifies"
    // this back to Process.Start("kill", ...) reproduces that hang.
    private static void Signal(int pid, int signal)
    {
        var rc = kill(pid, signal);
        if (rc != 0)
            throw new InvalidOperationException($"kill({pid}, {signal}) returned {rc}, errno={Marshal.GetLastWin32Error()}");
    }
}
