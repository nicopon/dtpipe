namespace DtPipe.Core.Abstractions;

using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
public interface IExportObserver
{
    // Lifecycle / Info
    void ShowIntro(string provider, string output);
    void ShowConnectionStatus(bool connected, int? columnCount);
    void ShowPipeline(IEnumerable<string> transformerNames);
    void ShowTarget(string provider, string output);

    // Logging
    void LogMessage(string message);
    void LogWarning(string message);
    void LogError(Exception ex);

    // Hooks
    void OnHookExecuting(string hookName, string command);

    // Progress
    IExportProgress CreateProgressReporter(bool isInteractive, IReadOnlyList<(string Name, bool IsColumnar)> transformerModes, bool suppressLiveTui = false, string? branchName = null, bool suppressCompletionOutput = false);

    // Sample mode — rendering only. The run already happened, on the real execution path;
    // the observer is handed what it produced. The old name said "run", which is precisely
    // the confusion that let a second engine live behind this interface.
    Task RenderSampleReportAsync(object report, PipelineExecutionPlan? executionPlan, bool isInteractive, CancellationToken ct = default);

    // Column type inference suggestion (shown during --dry-run or --auto-column-types for text sources like CSV)
    void ShowColumnTypeInferenceSuggestion(IReadOnlyDictionary<string, string> suggestions, int sampleCount, bool applied = false);
}
