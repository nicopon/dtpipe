namespace DtPipe.Core.Abstractions;

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
	IExportProgress CreateProgressReporter(bool isInteractive, IEnumerable<string> transformerNames);

	// Dry Run
	Task RunDryRunAsync(IStreamReader reader, IReadOnlyList<IDataTransformer> pipeline, int count, IDataWriter? inspectionWriter, CancellationToken ct);
}
