using DtPipe.Core.Models;

namespace DtPipe.Feedback;

public sealed record BranchSummary(
	string? Alias,
	ExportMetrics Metrics,
	bool ReaderIsColumnar,
	List<(string Name, bool IsColumnar)> TransformerModes
);
