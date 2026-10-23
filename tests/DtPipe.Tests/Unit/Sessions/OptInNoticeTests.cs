using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Sessions;
using AwesomeAssertions;
using DtPipe.Tests.Helpers;
using Xunit;

namespace DtPipe.Tests.Unit.Sessions;

/// <summary>
/// The notice is the informed half of "informed opt-in". Once per session, because a warning
/// repeated every run stops being read — and one that has stopped being read is worse than
/// none: it becomes noise while leaving the impression the user was told.
/// </summary>
[Collection(SessionStateCollection.Name)]
public class OptInNoticeTests : IDisposable
{
    private readonly string _tmp;
    private readonly string? _savedState;
    private readonly SessionStore _session;
    private readonly RecordingObserver _observer = new();

    public OptInNoticeTests()
    {
        _tmp = Path.Combine(Path.GetTempPath(), $"dtpipe_notice_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tmp);
        _savedState = Environment.GetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable);
        Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, Path.Combine(_tmp, "state"));
        _session = new SessionStore(new SessionIdentity("notice", Path.Combine(_tmp, ".dtpipe"), SessionOrigin.Explicit));
    }

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(UserStatePaths.RootEnvironmentVariable, _savedState);
        if (Directory.Exists(_tmp)) Directory.Delete(_tmp, recursive: true);
    }

    [Fact]
    public void The_Notice_Names_Path_Retention_Encryption_And_How_To_Purge()
    {
        OptInNotice.ShowOnce(_session, _observer, silenced: false);

        var text = string.Join("\n", _observer.Messages);
        text.Should().Contain(_session.SessionPath, "the user must be able to go and look");
        text.Should().Contain("AES-GCM").And.Contain(UserStatePaths.KeysDirectory());
        text.Should().Contain("days");
        text.Should().Contain("dtpipe session purge", "a notice without a remedy is just an alarm");
    }

    [Fact]
    public void The_Notice_Is_Shown_Once_Per_Session()
    {
        OptInNotice.ShowOnce(_session, _observer, silenced: false);
        OptInNotice.ShowOnce(_session, _observer, silenced: false);
        OptInNotice.ShowOnce(_session, _observer, silenced: false);

        _observer.Messages.Should().HaveCount(1);
    }

    [Fact]
    public void A_Silenced_Run_Shows_Nothing_And_Records_Nothing()
    {
        OptInNotice.ShowOnce(_session, _observer, silenced: true);

        _observer.Messages.Should().BeEmpty();
        OptInNotice.ShowOnce(_session, _observer, silenced: false);
        _observer.Messages.Should().HaveCount(1, "being silenced must not consume the one showing");
    }

    private sealed class RecordingObserver : IExportObserver
    {
        public List<string> Messages { get; } = new();
        public void LogMessage(string message) => Messages.Add(message);
        public void ShowIntro(string provider, string output) { }
        public void ShowConnectionStatus(bool connected, int? columnCount) { }
        public void ShowPipeline(IEnumerable<string> transformerNames) { }
        public void ShowTarget(string provider, string output) { }
        public void LogWarning(string message) { }
        public void LogError(Exception ex) { }
        public void OnHookExecuting(string hookName, string command) { }
        public IExportProgress CreateProgressReporter(bool isInteractive, IReadOnlyList<(string Name, bool IsColumnar)> transformerModes, bool suppressLiveTui = false, string? branchName = null, bool suppressCompletionOutput = false) => null!;
        public Task RenderSampleReportAsync(object report, PipelineExecutionPlan? executionPlan, bool isInteractive, CancellationToken ct = default) => Task.CompletedTask;
        public void ShowColumnTypeInferenceSuggestion(IReadOnlyDictionary<string, string> suggestions, int sampleCount, bool applied = false) { }
    }
}
