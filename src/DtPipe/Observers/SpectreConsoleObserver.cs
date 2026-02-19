using DtPipe.Core.Abstractions;
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
		var table = new Table();
		table.Border(TableBorder.None);
		table.AddColumn(new TableColumn("[grey]Source[/]").RightAligned());
		table.AddColumn(new TableColumn($"[blue]{provider}[/]"));
		_console.Write(table);
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
		_console.WriteLine();
		_console.Write(new Rule("[yellow]Pipeline[/]").LeftJustified());
		var grid = new Grid();
		grid.AddColumn();
		foreach (var name in transformerNames)
		{
			grid.AddRow($"[yellow]↓[/] [cyan]{name}[/]");
		}
		_console.Write(grid);
	}

	public void ShowTarget(string provider, string output)
	{
		var targetTable = new Table();
		targetTable.Border(TableBorder.None);
		targetTable.AddColumn(new TableColumn("[grey]Target[/]").RightAligned());
		targetTable.AddColumn(new TableColumn($"[blue]{provider}[/]"));

		if (!string.IsNullOrEmpty(output))
		{
			targetTable.AddColumn(new TableColumn($"([grey]{output}[/])"));
		}

		_console.Write(targetTable);
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

	public IExportProgress CreateProgressReporter(bool isInteractive, IEnumerable<string> transformerNames)
	{
		return new ProgressReporter(_console, isInteractive, transformerNames);
	}

	public async Task RunDryRunAsync(IStreamReader reader, IReadOnlyList<IDataTransformer> pipeline, int count, IDataWriter? inspectionWriter, CancellationToken ct)
	{
		// Explicitly using the CLI controller here
		var controller = new DtPipe.Cli.DryRun.DryRunCliController(_console);
		// Controller expects List<IDataTransformer>, so we convert.
		await controller.RunAsync(reader, pipeline.ToList(), count, inspectionWriter, ct);
	}

	public void ShowSchemaInfo(int columnCount)
	{
		// Consolidated into ShowConnectionStatus or separate?
		// ShowConnectionStatus handles it.
	}
}
