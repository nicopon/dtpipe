using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Feedback;
using Spectre.Console;

namespace DtPipe.Observers;

public class SpectreConsoleObserver : IExportObserver
{
	private readonly IAnsiConsole _console;

	public SpectreConsoleObserver(IAnsiConsole console)
	{
		_console = console;
	}

	public void ShowIntro(string provider, string output)
	{
		_console.MarkupLine($"[grey]Source[/]  [blue]{Markup.Escape(provider)}[/]");
	}

	public void ShowConnectionStatus(bool connected, int? columnCount)
	{
		if (connected)
		{
			_console.MarkupLine($"   [grey]Connected. Schema: [green]{columnCount ?? 0}[/] columns.[/]");
		}
		else
		{
			_console.MarkupLine($"   [grey]Connecting...[/]");
		}
	}

	public void ShowPipeline(IEnumerable<string> transformerNames)
	{
		var namesList = transformerNames.ToList();
		if (namesList.Count == 0) return;

		_console.WriteLine();
		var tree = new Tree("[yellow]⚙️ Transformations[/]");
		foreach (var name in namesList)
		{
			tree.AddNode($"[cyan]{name}[/]");
		}
		_console.Write(tree);
	}

	public void ShowTarget(string provider, string output)
	{
		if (!string.IsNullOrEmpty(output))
			_console.MarkupLine($"[grey]Target[/]  [blue]{Markup.Escape(provider)}[/] [grey]({Markup.Escape(output)})[/]");
		else
			_console.MarkupLine($"[grey]Target[/]  [blue]{Markup.Escape(provider)}[/]");
	}

	public void LogMessage(string message)
	{
		_console.MarkupLine(message);
	}

	public void LogWarning(string message)
	{
		_console.MarkupLine($"[yellow]{Markup.Escape(message)}[/]");
	}

	public void LogError(Exception ex)
	{
		_console.WriteLine($"Error: {ex.Message}");
	}

	public void OnHookExecuting(string hookName, string command)
	{
		// Simple logging for hooks
		// hookName e.g. "Pre-Hook"
		_console.MarkupLine($"   [yellow]Executing {hookName}: {Markup.Escape(command)}[/]");
	}

	public IExportProgress CreateProgressReporter(bool isInteractive, IReadOnlyList<(string Name, bool IsColumnar)> transformerModes, bool suppressLiveTui = false, string? branchName = null, bool suppressCompletionOutput = false)
	{
		return new ProgressReporter(_console, isInteractive, transformerModes, suppressLiveTui, branchName, suppressCompletionOutput);
	}

	public async Task RunDryRunAsync(IStreamReader reader, IReadOnlyList<IDataTransformer> pipeline, int count, IDataWriter? inspectionWriter, IReadOnlyDictionary<IDataTransformer, (IReadOnlyList<PipeColumnInfo> In, IReadOnlyList<PipeColumnInfo> Out)>? precomputedSchemas = null, PipelineExecutionPlan? executionPlan = null, CancellationToken ct = default)
	{
		var controller = new DtPipe.Cli.DryRun.DryRunCliController(_console);
		await controller.RunAsync(reader, pipeline.ToList(), count, inspectionWriter, precomputedSchemas, executionPlan, ct);
	}

	public void ShowColumnTypeInferenceSuggestion(IReadOnlyDictionary<string, string> suggestions)
	{
		if (suggestions.Count == 0) return;
		var spec = string.Join(",", suggestions.Select(kv => $"{kv.Key}:{kv.Value}"));
		_console.MarkupLine($"[grey]Suggested --column-types: \"[yellow]{Markup.Escape(spec)}[/]\"[/]");
	}

	public void ShowSchemaInfo(int columnCount)
	{
		// Consolidated into ShowConnectionStatus or separate?
		// ShowConnectionStatus handles it.
	}
}
