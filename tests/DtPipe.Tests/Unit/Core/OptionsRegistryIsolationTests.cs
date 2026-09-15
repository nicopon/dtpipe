using DtPipe.Adapters.Csv;
using DtPipe.Core.Options;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// Tests for OptionsRegistry.BeginScope() — ensures concurrent DAG branches
/// each get an isolated copy of the options dictionary.
///
/// Background: OptionsRegistry uses AsyncLocal&lt;Dictionary&lt;Type, object&gt;&gt;.
/// AsyncLocal isolates reference assignments, but NOT in-place mutations of a
/// shared object. Without BeginScope(), branches inherit the same Dictionary
/// reference and overwrite each other's options.
/// BeginScope() performs a copy-on-write fork: it creates a new dict (copying
/// parent entries), then reassigns _options.Value — isolating all subsequent
/// writes to the current async context.
/// </summary>
public class OptionsRegistryIsolationTests
{
    [Fact]
    public async Task BeginScope_IsolatesConcurrentBranches()
    {
        var registry = new OptionsRegistry();
        var branchBDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        string? aRead = null;
        string? bRead = null;

        async Task BranchA()
        {
            await Task.Yield(); // mirrors ExecuteBranchAsync's await Task.Yield()
            registry.BeginScope();
            registry.Register(new CsvReaderOptions { ColumnTypes = "branch-A" });
            await branchBDone.Task; // wait until B has also written, to force the race
            aRead = registry.Get<CsvReaderOptions>().ColumnTypes;
        }

        async Task BranchB()
        {
            await Task.Yield();
            registry.BeginScope();
            registry.Register(new CsvReaderOptions { ColumnTypes = "branch-B" });
            branchBDone.SetResult();
            bRead = registry.Get<CsvReaderOptions>().ColumnTypes;
        }

        await Task.WhenAll(BranchA(), BranchB());

        Assert.Equal("branch-A", aRead); // branch A must not see branch B's value
        Assert.Equal("branch-B", bRead);
    }

    [Fact]
    public async Task BeginScope_InheritsParentOptions()
    {
        var registry = new OptionsRegistry();
        registry.Register(new CsvReaderOptions { Separator = "|" });

        string? separatorInBranch = null;

        async Task Branch()
        {
            await Task.Yield();
            registry.BeginScope();
            // branch does not override Separator — it should inherit the parent value
            separatorInBranch = registry.Get<CsvReaderOptions>().Separator;
        }

        await Branch();

        Assert.Equal("|", separatorInBranch);
    }

    [Fact]
    public async Task BeginScope_BranchWriteDoesNotLeakToSibling()
    {
        var registry = new OptionsRegistry();
        var bStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var aWritten = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        bool bFound = true;
        string? bRead = null;

        async Task BranchA()
        {
            await Task.Yield();
            registry.BeginScope();
            bStarted.SetResult();
            await Task.Yield(); // let B run its BeginScope
            registry.Register(new CsvReaderOptions { ColumnTypes = "only-A" });
            aWritten.SetResult();
        }

        async Task BranchB()
        {
            await bStarted.Task;
            await Task.Yield();
            registry.BeginScope();
            await aWritten.Task; // wait until A has written
            // TryGet (not Get): asserting the miss is a stronger check than "sees the default",
            // and it keeps the WarnMissing noise out of the build log.
            bFound = registry.TryGet<CsvReaderOptions>(out var bOpts);
            bRead = bOpts.ColumnTypes;
        }

        await Task.WhenAll(BranchA(), BranchB());

        Assert.False(bFound);    // B's scope was forked before A wrote — A's registration is invisible to B
        Assert.Equal("", bRead); // and the fallback default carries no leaked value
    }

    private sealed class ProbeOptions : IOptionSet
    {
        public static string Prefix => "probe";
        public static string DisplayName => "Probe";
    }

    /// <summary>
    /// A miss yields a usable default and says nothing. What it would have to say is a wiring
    /// condition, not a user's mistake, and it is asserted where it is decidable:
    /// OptionsRegistryCoverageTests, over the whole catalogue.
    /// </summary>
    [Fact]
    public void A_Miss_Returns_A_Default_Instance()
    {
        var registry = new OptionsRegistry();

        Assert.NotNull(registry.Get<ProbeOptions>());
    }

    [Fact]
    public void TryGet_Missing_Returns_False()
    {
        var registry = new OptionsRegistry();

        var found = registry.TryGet<ProbeOptions>(out var value);

        Assert.False(found);
        Assert.NotNull(value);
        Assert.False(registry.TryGet<CsvReaderOptions>(out _));
    }

    [Fact]
    public void TryGet_Hit_Returns_Registered_Instance()
    {
        var registry = new OptionsRegistry();
        var registered = new CsvReaderOptions { Separator = ";" };
        registry.Register(registered);

        var found = registry.TryGet<CsvReaderOptions>(out var value);

        Assert.True(found);
        Assert.Same(registered, value);
    }

    [Fact]
    public void Require_Missing_Throws()
    {
        var registry = new OptionsRegistry();

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Require<ProbeOptions>());

        Assert.Contains("ProbeOptions", ex.Message);
    }

    [Fact]
    public void Require_Hit_Returns_Registered_Instance()
    {
        var registry = new OptionsRegistry();
        var registered = new CsvReaderOptions { Separator = ";" };
        registry.Register(registered);

        Assert.Same(registered, registry.Require<CsvReaderOptions>());
    }
}
