using System.Collections.Concurrent;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Feedback;
using Spectre.Console;

namespace DtPipe.Observers;

public class SpectreConsoleObserver : IExportObserver
{
	private readonly IAnsiConsole _console;
	private readonly ConcurrentDictionary<string, ProgressReporter> _activeReporters = new();

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
		{
			var safeOutput = DtPipe.Core.Security.ConnectionStringSanitizer.Sanitize(output);
			_console.MarkupLine($"[grey]Target[/]  [blue]{Markup.Escape(provider)}[/] [grey]({Markup.Escape(safeOutput)})[/]");
		}
		else
		{
			_console.MarkupLine($"[grey]Target[/]  [blue]{Markup.Escape(provider)}[/]");
		}
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
		var reporter = new ProgressReporter(_console, isInteractive, transformerModes, suppressLiveTui, branchName, suppressCompletionOutput);
		if (branchName != null)
		{
			_activeReporters.TryAdd(branchName, reporter);
		}
		return reporter;
	}

	public async Task RunDryRunAsync(IStreamReader reader, IReadOnlyList<IDataTransformer> pipeline, int count, IDataWriter? inspectionWriter, IReadOnlyDictionary<IDataTransformer, (IReadOnlyList<PipeColumnInfo> In, IReadOnlyList<PipeColumnInfo> Out)>? precomputedSchemas = null, PipelineExecutionPlan? executionPlan = null, bool isInteractive = true, CancellationToken ct = default)
	{
		var controller = new DtPipe.Cli.DryRun.DryRunCliController(_console);
		await controller.RunAsync(reader, pipeline.ToList(), count, inspectionWriter, precomputedSchemas, executionPlan, isInteractive, ct);
	}

	public void ShowColumnTypeInferenceSuggestion(IReadOnlyDictionary<string, string> suggestions, int sampleCount, bool applied = false)
	{
		if (suggestions.Count == 0) return;
		var spec = string.Join(",", suggestions.Select(kv => $"{kv.Key}:{kv.Value}"));
		string body = applied
			? $"Applied: [yellow]--column-types \"{Markup.Escape(spec)}\"[/]"
			: $"Add to your command: [yellow]--column-types \"{Markup.Escape(spec)}\"[/]";
		string header = applied
			? $" [green]Column types auto-applied[/] [grey](sampled {sampleCount} rows)[/] "
			: $" [yellow]Type inference suggestion[/] [grey](sampled {sampleCount} rows)[/] ";
		var panel = new Panel(body)
		{
			Header = new PanelHeader(header),
			Border = BoxBorder.Rounded,
			Padding = new Padding(1, 0)
		};
		_console.Write(panel);
	}

	public void ShowSchemaInfo(int columnCount)
	{
		// Consolidated into ShowConnectionStatus or separate?
		// ShowConnectionStatus handles it.
	}

	public async Task<int> StartUnifiedLiveDisplayAsync(JobDagDefinition dagDefinition, Func<Task<int>> executionAction, CancellationToken ct)
	{
		var branchOrder = dagDefinition.Branches
			.Select((b, i) => (b.Alias, Index: i))
			.ToDictionary(x => x.Alias, x => x.Index, StringComparer.OrdinalIgnoreCase);

		int exitCode = 0;

		await _console.Live(BuildUnifiedTable(branchOrder))
			.AutoClear(false)
			.Overflow(VerticalOverflow.Ellipsis)
			.Cropping(VerticalOverflowCropping.Bottom)
			.StartAsync(async ctx =>
			{
				// Run the DAG execution in the background while updating the UI
				var execTask = executionAction();

				while (!execTask.IsCompleted)
				{
					ctx.UpdateTarget(BuildUnifiedTable(branchOrder));
					try { await Task.Delay(500, ct); } catch (TaskCanceledException) { break; }
				}

				try { exitCode = await execTask; } catch { exitCode = 1; }
				ctx.UpdateTarget(BuildUnifiedTable(branchOrder));
			});
			
		return exitCode;
	}

	private Table BuildUnifiedTable(Dictionary<string, int> branchOrder)
	{
		var reporters = _activeReporters.Values
			.OrderBy(r => r.BranchName != null && branchOrder.TryGetValue(r.BranchName, out var idx) ? idx : int.MaxValue)
			.ToList();

		var table = new Table().Border(TableBorder.Rounded);
		table.Title = new TableTitle("[yellow] Live Execution [/]");

		table.AddColumn(new TableColumn("[grey]Branch[/]"));
		table.AddColumn(new TableColumn("[grey]Stage[/]"));
		table.AddColumn(new TableColumn("[grey]Rows[/]").RightAligned());
		table.AddColumn(new TableColumn("[grey]Speed[/]").RightAligned());
		
		bool hasMode = reporters.Any(r => r.TransformerModes.Count > 0);
		if (hasMode) table.AddColumn(new TableColumn("[grey]Mode[/]"));

		void AddRow(string branch, string stage, string rows, string speed, string mode)
		{
			var cols = new List<string> { branch, stage, rows, speed };
			if (hasMode) cols.Add(mode);
			table.AddRow(cols.ToArray());
		}

		bool firstBranch = true;
		foreach (var r in reporters)
		{
			if (!firstBranch) AddRow("", "", "", "", "");
			firstBranch = false;

			double elapsed = r.Elapsed.TotalSeconds;
			string branchLabel = r.BranchName != null ? $"[white][[{Markup.Escape(r.BranchName)}]][/]" : "";
			
			AddRow(branchLabel, "[grey]▸ Reading[/]", $"[white]{r.ReadCount:N0}[/]", $"[grey]{FormatSpeed(r.ReadCount, elapsed)}[/]", "");

			var indexedCounts = r.TransformerCountsByIndex;
			for (int ti = 0; ti < r.TransformerModes.Count; ti++)
			{
				var (name, isColumnar) = r.TransformerModes[ti];
				long count = indexedCounts != null && ti < indexedCounts.Length
					? indexedCounts[ti]
					: (r.TransformerStats.TryGetValue(name, out var c) ? c : 0);
				string modeLbl = isColumnar ? "[cyan]◈ columnar[/]" : "[yellow]● row[/]";
				AddRow("", $"[grey]▸ → {Markup.Escape(name)}[/]", $"[white]{count:N0}[/]", $"[grey]{FormatSpeed(count, elapsed)}[/]", modeLbl);
			}

			AddRow("", "[grey]▸ Writing[/]", $"[white]{r.WriteCount:N0}[/]", $"[grey]{FormatSpeed(r.WriteCount, elapsed)}[/]", "");
		}

		if (reporters.Count == 0)
		{
			AddRow("[grey]Waiting for branches to initialize...[/]", "", "", "", "");
		}

		return table;
	}

	private static string FormatSpeed(long count, double elapsedSeconds)
	{
		double rps = elapsedSeconds > 0 ? count / elapsedSeconds : 0;
		return rps switch
		{
			>= 1_000_000 => $"{rps / 1_000_000:F1}M/s",
			>= 1_000 => $"{rps / 1_000:F1}K/s",
			_ => $"{rps:F0}/s"
		};
	}
}
